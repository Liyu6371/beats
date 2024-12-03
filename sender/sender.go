package sender

import (
	"beats/config"
	"beats/logger"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var senderCtrl *Controller

type Controller struct {
	senderNames []string
	wg          sync.WaitGroup
	mu          sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	ch             chan Msg
	senderInstance map[string]Instance
}

func New(senders []string) *Controller {
	if len(senders) == 0 {
		logger.Errorln("sender controller: sender instance name should not be empty")
		return nil
	}

	if len(instanceFactory) == 0 {
		logger.Errorln("sender controller: sender factory should not be empty")
		return nil
	}

	senderCtrl = &Controller{
		senderNames:    senders,
		wg:             sync.WaitGroup{},
		mu:             sync.RWMutex{},
		ch:             make(chan Msg, 2000),
		senderInstance: make(map[string]Instance),
	}

	return senderCtrl
}

func (c *Controller) Run(parent context.Context) error {
	c.ctx, c.cancel = context.WithCancel(parent)

	for _, name := range c.senderNames {
		fn, ok := instanceFactory[name]
		if !ok {
			return fmt.Errorf("sender controller: sender instance %s not found", name)
		}
		// 在这里，对于没有 enabled 的实例， 返回的 instance、error 都是 nil
		instance, err := fn(config.GetConf().Sender)
		if err != nil {
			return fmt.Errorf("sender controller: sender instance %s error: %v", name, err)
		}
		if instance == nil {
			continue
		}
		if err := instance.RunSender(c.ctx); err != nil {
			return fmt.Errorf("sender controller: run sender instance %s error: %v", name, err)
		}
		c.mu.Lock()
		c.senderInstance[name] = instance
		c.mu.Unlock()
	}

	var length int
	c.mu.RLock()
	length = len(c.senderInstance)
	c.mu.RUnlock()
	// 校验是否有具体的 sender 实例被拉起执行， 如果没有的话直接返回
	if length == 0 {
		return errors.New("sender controller: no sender instances running, maybe something wrong")
	}
	// 拉起消息分发
	for i := 0; i < 3; i++ {
		c.wg.Add(1)
		go c.dispatch()
	}

	c.wg.Add(1)
	go c.check()

	return nil
}

func (c *Controller) dispatch() {
	defer func() {
		c.wg.Done()
	}()
	for {
		select {
		case msg, ok := <-c.ch:
			if !ok {
				return
			}
			c.mu.RLock()
			instance, ok := c.senderInstance[msg.GetSender()]
			c.mu.RUnlock()
			if !ok {
				logger.Errorf("sender controller: sender instance %s found", msg.GetSender())
				continue
			}
			instance.Push(msg)
			logger.Debugf("sender controller: send message to %s with data: %s", msg.GetSender(), msg.GetData())
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Controller) check() {
	defer c.wg.Done()
	ticker := time.NewTicker(time.Minute * 5)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.mu.RLock()
			for name, instance := range c.senderInstance {
				if !instance.Alive() {
					logger.Errorf("sender controller: sender instance %s is dead", name)
				}
			}
			c.mu.RUnlock()
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Controller) Stop() {
	c.cancel()
	c.wg.Wait()
	close(c.ch)
	logger.Infoln("sender controller: stopped")
}

func GetController() *Controller {
	return senderCtrl
}
