package beat

import (
	"beats/config"
	"beats/logger"
	"beats/sender"
	"beats/source"
	"beats/task"
	"beats/utils"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	_ "beats/register"
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
	// 尝试拉起 source Controller
	var sCtrl *source.Controller
	if sCtrl = source.New(); sCtrl != nil {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			sCtrl.Run(b.ctx)
		}()
	}

	// 尝试拉起 sender Controller
	// senderCtrl 作为数据的出口必须存在，并且最少存在一个 sender 实例
	senderCtrl := sender.New()
	if senderCtrl == nil {
		logger.Errorln("senderCtrl is nil, program exit")
		return
	}
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		senderCtrl.Run(b.ctx)
	}()

	time.Sleep(time.Second * 5)

	// 开始处理任务
	// 周期任务
	if len(config.GetConf().PeriodTask) > 0 {
		for name, taskConf := range config.GetConf().PeriodTask {
			if pTask := task.NewPeriodTask(name, taskConf); pTask != nil {
				b.wg.Add(1)
				go func() {
					defer b.wg.Done()
					pTask.Run(b.ctx, nil, senderCtrl.Channel())
				}()
			}
		}
	}

	// 非周期任务
	if len(config.GetConf().NonPeriodTask) > 0 && sCtrl != nil {
		for name, taskConf := range config.GetConf().NonPeriodTask {
			if pTask := task.NewNonPeriodTask(name, taskConf); pTask != nil {
				b.wg.Add(1)
				go func() {
					defer b.wg.Done()
					pTask.Run(b.ctx, sCtrl.GetChannel(), senderCtrl.Channel())
				}()
			}
		}
	}
	// 生成 pid 文件
	if err := utils.GenPid(); err != nil {
		logger.Errorf("unable to GenPid, error: %s", err)
		return
	}
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	b.cancel()
	b.wg.Wait()
	logger.Infoln("program exit")
}
