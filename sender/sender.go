package sender

import (
	"beats/config"
	"beats/logger"
	"context"
	"os"
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

func New() *Controller {
	sConf := config.GetConf().Sender
	if len(sConf) == 0 {
		logger.Errorln("sender controller: sender configuration should not be empty")
		return nil
	}

	if len(instanceFactory) == 0 {
		logger.Errorln("sender controller: sender factory should not be empty")
		return nil
	}

	var senders []string
	for k, _ := range sConf {
		senders = append(senders, k)
	}

	senderCtrl = &Controller{
		senderNames:    senders,
		wg:             sync.WaitGroup{},
		mu:             sync.RWMutex{},
		senderInstance: make(map[string]Instance),
	}

	return senderCtrl
}

func (c *Controller) Run(parent context.Context) {
	c.ch = make(chan Msg, 2000)
	defer close(c.ch)

	c.ctx, c.cancel = context.WithCancel(parent)
	for _, name := range c.senderNames {
		fn, ok := instanceFactory[name]
		if !ok {
			logger.Errorf("sender controller: sender instance %s not found", name)
			continue
		}
		// 在这里，对于没有 enabled 的实例， 返回的 instance、error 都是 nil
		instance := fn(config.GetConf().Sender)
		if instance == nil {
			continue
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			instance.RunSender(c.ctx)
		}()
		// 记录拉起的 sender 实例
		c.senderInstance[name] = instance
	}

	// 判断一下是否有 sender 实例被拉起来
	time.Sleep(time.Second * 1)
	var senderFlag bool
	for name, sender := range c.senderInstance {
		if !sender.Alive() {
			logger.Errorf("sender controller: sender instance %s is dead", name)
			continue
		}
		senderFlag = true
	}

	// 如果一个实例都没被拉起来，那么就直接 os.exit()
	if !senderFlag {
		logger.Errorln("sender controller: no sender instance is alive, program exit")
		os.Exit(1)
	}

	// 拉起消息分发
	for i := 0; i < 3; i++ {
		c.wg.Add(1)
		go c.dispatch()
	}

	c.wg.Add(1)
	go c.check()

	<-c.ctx.Done()
	c.wg.Wait()
	logger.Infoln("sender Controller exit")

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
				logger.Errorf("sender controller: sender instance %s not found", msg.GetSender())
				continue
			}
			// 判定对应的 sender 实例是否存活
			if !instance.Alive() {
				logger.Errorf("sender controller: sender instance %s is dead", msg.GetSender())
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
	logger.Infoln("sender controller: send exit signal")
}

func (c *Controller) Channel() chan<- Msg {
	return c.ch
}
