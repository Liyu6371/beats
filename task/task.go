package task

import (
	"beats/config"
	"beats/logger"
	"beats/pipeline"
	"beats/sender"
	"context"
	"time"
)

type PeriodTask struct {
	name   string
	conf   config.Task
	period time.Duration
	pipe   *pipeline.Pipeline

	ctx    context.Context
	cancel context.CancelFunc
}

func NewPeriodTask(name string, conf config.Task) *PeriodTask {
	p, ok := conf["period"].(string)
	if !ok {
		logger.Errorln("period keyword not found")
		return nil
	}
	var (
		interval time.Duration
		err      error
	)
	if interval, err = time.ParseDuration(p); err != nil {
		logger.Errorf("unable to parse duration, error: %s", err)
		return nil
	}
	pipe := pipeline.New(conf)
	if pipe == nil {
		logger.Errorln("unable to new pipeline")
		return nil
	}
	return &PeriodTask{
		name:   name,
		conf:   conf,
		period: interval,
		pipe:   pipe,
	}
}

func (t *PeriodTask) Run(parent context.Context, input <-chan []byte, output chan<- sender.Msg) {
	t.ctx, t.cancel = context.WithCancel(parent)
	ticker := time.NewTicker(t.period)
	defer ticker.Stop()
	var perTask bool
	for {
		select {
		case <-ticker.C:
			if !perTask {
				perTask = true
				// 非阻塞的 pipeline， 运行完毕后自动退出
				go func() {
					t.pipe.Run(t.ctx, input, output)
					perTask = false
				}()
			}
		case <-t.ctx.Done():
			return
		}
	}
}

type NonPeriodTask struct {
	name string
	conf config.Task
	pipe *pipeline.Pipeline

	ctx    context.Context
	cancel context.CancelFunc
}

func NewNonPeriodTask(name string, conf config.Task) *NonPeriodTask {
	pipe := pipeline.New(conf)
	if pipe == nil {
		logger.Errorln("nonPeriodTask: unable to new pipeline")
		return nil
	}

	return &NonPeriodTask{
		name: name,
		conf: conf,
		pipe: pipe,
	}
}

func (t *NonPeriodTask) Run(parent context.Context, input <-chan []byte, output chan<- sender.Msg) {
	t.ctx, t.cancel = context.WithCancel(parent)
	t.pipe.Run(t.ctx, input, output)
}
