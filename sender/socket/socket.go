package socket

import (
	"beats/logger"
	"beats/sender"
	"context"
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/libgse/gse"
	"github.com/mitchellh/mapstructure"
	"sync"
)

const (
	Name = "socket"
)

func init() {
	sender.RegisterInstance(Name, New)
}

// SocketSender GSE 发送者
type SocketSender struct {
	c      Config
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ch     chan sender.Msg
	state  bool
	ctx    context.Context
	cancel context.CancelFunc
	client *gse.GseClient
}

func New(conf map[string]interface{}) sender.Instance {
	v, ok := conf[Name]
	if !ok {
		logger.Errorln("socket sender instance: config not found")
		return nil
	}
	c := Config{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("socket sender instance: decode error: %s", err)
		return nil
	}
	if !c.Enabled {
		logger.Warnln("socket sender instance: not enabled")
		return nil
	}
	if c.EndPoint != "" {
		defaultGseConf.Endpoint = c.EndPoint
	}
	client, err := gse.NewGseClientFromConfig(defaultGseConf)
	if err != nil {
		logger.Errorf("socket sender instance: create client error: %s", err)
		return nil
	}

	var ch chan sender.Msg
	if c.Buffer != 0 {
		ch = make(chan sender.Msg, c.Buffer)
	} else {
		ch = make(chan sender.Msg, buffer)
	}

	return &SocketSender{
		c:      c,
		mu:     sync.RWMutex{},
		wg:     sync.WaitGroup{},
		ch:     ch,
		state:  false,
		client: client,
	}
}

func (s *SocketSender) GetName() string {
	return Name
}

func (s *SocketSender) Alive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *SocketSender) turnOn() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.state {
		s.state = true
	}
}

func (s *SocketSender) turnOff() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state {
		s.state = false
	}
}

func (s *SocketSender) RunSender(parent context.Context) {
	defer close(s.ch)
	if err := s.client.Start(); err != nil {
		logger.Errorf("socket sender instance: start client error: %s", err)
		return
	}
	defer s.client.Close()
	s.ctx, s.cancel = context.WithCancel(parent)
	s.turnOn()
	defer s.turnOff()

	var workerNum int
	if s.c.Worker != 0 {
		workerNum = s.c.Worker
	} else {
		workerNum = 3
	}

	for i := 0; i < workerNum; i++ {
		s.wg.Add(1)
		go s.consume(i)
	}

	<-s.ctx.Done()
	s.wg.Wait()
	logger.Infof("socket sender instance: stopped")
}

func (s *SocketSender) consume(idx int) {
	defer func() {
		s.wg.Done()
	}()

	for {
		select {
		case msg, ok := <-s.ch:
			if !ok {
				logger.Warnf("socket sender instance: channel closed, consumer goroutine %d exot", idx)
				return
			}
			dataid, ok := msg.Where().(int32)
			if !ok {
				logger.Errorf("socket sender instance: transform error, data id is %+v", msg.Where())
				logger.Debugf("socket sender instance: transform error, data id is %+v, drop data: %s", msg.Where(), msg.GetData())
				continue
			}
			if err := s.client.Send(gse.NewGseCommonMsg(msg.GetData(), dataid, 0, 0, 0)); err != nil {
				logger.Errorf("socket sender instance: send data error: %s", err)
				logger.Debugf("socket sender instance: send data error: %s, dataid: %d, data: %s", err, dataid, msg.GetData())
				continue
			}
		case <-s.ctx.Done():
			logger.Infof("socket sender instance: stop consumer goroutine %d", idx)
			return
		}
	}
}

func (s *SocketSender) StopSender() {
	s.cancel()
	logger.Infoln("socket sender instance:  send stop signal")
}

func (s *SocketSender) Push(msg sender.Msg) {
	s.ch <- msg
}
