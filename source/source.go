package source

import (
	"beats/config"
	"beats/logger"
	"context"
	"fmt"
	"sync"
)

var sourceCtrl *Controller

type Controller struct {
	sourceNames []string
	wg          sync.WaitGroup
	mu          sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc

	ch             chan []byte
	sourceInstance map[string]Instance
}

func New(sources []string) *Controller {
	if len(sources) == 0 {
		logger.Warnf("source controller: no source name found")
		return nil
	}
	if len(instanceFactory) == 0 {
		logger.Errorf("source controller: no source instance found")
		return nil
	}

	sourceCtrl = &Controller{
		sourceNames:    sources,
		wg:             sync.WaitGroup{},
		mu:             sync.RWMutex{},
		ch:             make(chan []byte, 2000),
		sourceInstance: make(map[string]Instance),
	}

	return sourceCtrl
}

func (c *Controller) Run(parent context.Context) error {
	c.ctx, c.cancel = context.WithCancel(parent)

	for _, sourceName := range c.sourceNames {
		fn, ok := instanceFactory[sourceName]
		if !ok {
			return fmt.Errorf("source controller: source name %s not found", sourceName)
		}
		instance, err := fn(config.GetConf().Source)
		if err != nil {
			return fmt.Errorf("source controller: sourceInstance %s error: %v", sourceName, err)
		}
		if instance == nil {
			continue
		}
		if err := instance.StartSource(c.ctx, c.ch); err != nil {
			return fmt.Errorf("source controller: start instance %s error: %v", sourceName, err)
		}
		c.mu.Lock()
		c.sourceInstance[sourceName] = instance
		c.mu.Unlock()
	}

	return nil
}

func (c *Controller) GetChannel() <-chan []byte {
	return c.ch
}

func (c *Controller) Stop() {
	c.cancel()
	c.wg.Wait()
	close(c.ch)
	logger.Infoln("source controller stopped")
}

func GetController() *Controller {
	return sourceCtrl
}
