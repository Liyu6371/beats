package beat

import (
	"beats/config"
	"beats/logger"
	"beats/source"
	"context"
	"fmt"
	"os"
	"sync"
)

type Beat struct {
	c  *config.Config
	wg sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
}

func New(path string) *Beat {
	// 初始化配置
	if err := config.InitConfig(path); err != nil {
		fmt.Printf("init config error: %s\n", err)
		return nil
	}
	// 初始化日志
	logger.InitLogger(config.GetConf().Logger)

	// 初始化 ctx
	ctx, cancel := context.WithCancel(context.Background())
	return &Beat{
		c:      config.GetConf(),
		wg:     sync.WaitGroup{},
		ctx:    ctx,
		cancel: cancel,
	}
}

func (b *Beat) Start() {
	if len(b.c.Source) > 0 {
		var sName []string
		for name := range b.c.Source {
			sName = append(sName, name)
		}
		if sCtrl := source.New(sName); sCtrl == nil {
			logger.Errorf("source ctrl is nil")
			os.Exit(1)
		} else {

		}
	}
}

func (b *Beat) Stop() {}
