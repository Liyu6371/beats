package source

import (
	"beats/config"
	"beats/logger"
	"context"
	"sync"
)

var sourceCtrl *Controller

type Controller struct {
	sourceNames []string

	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup

	ch             chan []byte
	sourceInstance map[string]Instance
}

func New() *Controller {
	sConf := config.GetConf().Source
	if len(sConf) == 0 {
		logger.Warnf("source controller: no source configuration found")
		return nil
	}

	if len(instanceFactory) == 0 {
		logger.Errorf("source controller: no source instance registed")
		return nil
	}

	var sources []string
	for k, _ := range sConf {
		sources = append(sources, k)
	}

	sourceCtrl = &Controller{
		sourceNames:    sources,
		wg:             sync.WaitGroup{},
		ch:             make(chan []byte, 2000),
		sourceInstance: make(map[string]Instance),
	}

	return sourceCtrl
}

func (c *Controller) Run(parent context.Context) {
	defer close(c.ch)
	c.ctx, c.cancel = context.WithCancel(parent)

	for _, sourceName := range c.sourceNames {
		fn, ok := instanceFactory[sourceName]
		if !ok {
			logger.Errorf("source controller: source name %s not found", sourceName)
			continue
		}
		// 创建 instance 实例
		instance := fn(config.GetConf().Source)
		if instance == nil {
			continue
		}
		// 通过 goroutine 的形式将 source 实例拉起
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			instance.StartSource(c.ctx, c.ch)

		}()
		// 记录拉起的实例
		c.sourceInstance[sourceName] = instance
	}

	// 等待上层退出信号
	<-parent.Done()
	// 再次调用 cancel 确保资源以及拉起的 goroutine 释放
	c.cancel()
	c.wg.Wait()
	logger.Infoln("source Controller exit")
}

func (c *Controller) GetChannel() <-chan []byte {
	return c.ch
}
