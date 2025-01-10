package pipeline

import (
	"beats/logger"
	"beats/sender"
	"context"
	"sync"

	"github.com/mitchellh/mapstructure"
)

var (
	Name   = "pipeline"
	worker = 3
)

type Pipeline struct {
	input  <-chan []byte
	output chan<- sender.Msg

	ctx    context.Context
	cancel context.CancelFunc

	wg         sync.WaitGroup
	processors []Processor
	shapers    []Shaper
}

type pipeConf struct {
	Processor []string `mapstructure:"processor"`
	Shaper    []string `mapstructure:"shaper"`
}

func New(conf map[string]interface{}) *Pipeline {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("pipeline: config not found")
		return nil
	}
	c := pipeConf{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("pipeline: decode config error: %s", err)
		return nil
	}
	var processor []Processor
	for _, name := range c.Processor {
		fn, ok := processorFactory[name]
		if !ok {
			logger.Errorf("pipeline: processor not found: %s", name)
			return nil
		}
		processor = append(processor, fn())
	}

	var shaper []Shaper
	for _, name := range c.Shaper {
		fn, ok := shaperFactory[name]
		if !ok {
			logger.Errorf("pipeline: shaper not found: %s", name)
			return nil
		}
		shaper = append(shaper, fn())
	}
	return &Pipeline{
		wg:         sync.WaitGroup{},
		processors: processor,
		shapers:    shaper,
	}
}

func (p *Pipeline) Run(parent context.Context, input <-chan []byte, output chan<- sender.Msg) {
	p.ctx, p.cancel = context.WithCancel(parent)
	p.input, p.output = input, output
	// 存在 input 说明是有 source 类型的任务
	if input != nil {
		for {
			select {
			case msg, ok := <-p.input:
				if !ok {
					return
				}
				//fmt.Printf("pipeline goroutine receive msg: %s\n ", msg)
				p.process(msg)
			case <-p.ctx.Done():
				return
			}
		}
	} else {
		p.processWithoutInput()
	}
}

func (p *Pipeline) process(msg []byte) {
	var (
		data interface{}
		err  error
	)
	data = msg
	for _, processor := range p.processors {
		data, err = processor.Process(data)
		if err != nil {
			logger.Errorf("pipeline: processor: %s process data error: %s", processor.GetName(), err)
			return
		}
	}

	for _, shaper := range p.shapers {
		sendMsgs, err := shaper.Shape(data)
		if err != nil {
			logger.Errorf("pipeline: shaper %s shape error: %s", shaper.GetName(), err)
			logger.Debugf("pipeline: shaper %s shape error: %s, data: %s", shaper.GetName(), err, data)
			continue
		}
		for _, item := range sendMsgs {
			p.output <- item
		}
	}
}

func (p *Pipeline) processWithoutInput() {
	head := p.processors[0]
	data, err := head.Process(nil)
	if err != nil {
		logger.Errorf("pipeline: head processor error: %s", err)
		return
	}

	for _, processor := range p.processors[1:] {
		data, err = processor.Process(data)
		if err != nil {
			logger.Errorf("pipeline: processor: %s exec error: %s", processor.GetName(), err)
			return
		}
	}

	for _, shaper := range p.shapers {
		sendMsgs, err := shaper.Shape(data)
		if err != nil {
			logger.Errorf("pipeline: shaper: %s shape data error: %s", shaper.GetName(), err)
			logger.Debugf("pipeline: shaper: %s shape data: %+v error: %s", shaper.GetName(), data, err)
			continue
		}
		for _, item := range sendMsgs {
			p.output <- item
		}
	}
}
