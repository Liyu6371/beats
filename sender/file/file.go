package file

import (
	"beats/logger"
	"beats/sender"
	"context"
	"os"
	"sync"

	"github.com/mitchellh/mapstructure"
)

const (
	Name = "file"
)

func init() {
	sender.RegisterInstance(Name, New)
}

type FileSender struct {
	c Config

	ch    chan sender.Msg
	mu    sync.RWMutex
	state bool
}

func New(conf map[string]interface{}) sender.Instance {
	v, ok := conf[Name]
	if !ok {
		logger.Errorf("file sender instance: config not found")
		return nil
	}
	c := Config{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("file sender instance: unable to decode conf, error: %s", err)
		return nil
	}
	if !c.Enabled {
		logger.Warnf("file sender instance: not enabled")
		return nil
	}

	var ch chan sender.Msg
	if c.Buffer != 0 {
		ch = make(chan sender.Msg, c.Buffer)
	} else {
		ch = make(chan sender.Msg, 500)
	}

	return &FileSender{
		ch:    ch,
		mu:    sync.RWMutex{},
		state: false,
	}
}

func (f *FileSender) Push(msg sender.Msg) {
	f.ch <- msg
}

func (f *FileSender) RunSender(parent context.Context) {
	f.turnOn()
	defer func() {
		f.turnOff()
		close(f.ch)
	}()

	for {
		select {
		case msg, ok := <-f.ch:
			if !ok {
				return
			}
			path, ok := msg.Where().(string)
			if !ok {
				logger.Errorf("file sender instance: failed to transform write path, origin: %+v", path)
				continue
			}
			f.writeFile(msg.GetData(), path)
		case <-parent.Done():
			return
		}
	}

}

func (f *FileSender) StopSender() {
	logger.Warnf("file sender instance: stopSender no implement.")
}

func (f *FileSender) Alive() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.state
}

func (f *FileSender) turnOn() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.state {
		f.state = true
	}
}

func (f *FileSender) turnOff() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.state {
		f.state = false
	}
}

func (f *FileSender) GetName() string {
	return Name
}

func (f *FileSender) writeFile(data []byte, path string) {
	var (
		file *os.File
		err  error
	)

	if f.c.AppendMode {
		file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	}
	if err != nil {
		logger.Errorf("file sender instance: failed to open file: %s, error: %s", path, err)
		logger.Debugf("file sender instance: drop data: %s", data)
		return
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		logger.Errorf("write file error: %s", err)
		logger.Debugf("file sender instance: write file error: %s, drop data: %s", err, data)
		return
	}
	logger.Debugln("write file success")
}
