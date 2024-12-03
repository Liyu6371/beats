package kafka

import (
	"beats/logger"
	"beats/sender"
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mitchellh/mapstructure"
	"strings"
	"sync"
)

const (
	Name = "kafka"
)

func init() {
	sender.RegisterInstance(Name, New)
}

// KafkaSender kafka 发送者
type KafkaSender struct {
	c      Config
	mu     sync.RWMutex
	wg     sync.WaitGroup
	ch     chan sender.Msg
	state  bool
	ctx    context.Context
	cancel context.CancelFunc
	client sarama.SyncProducer
}

func New(conf map[string]interface{}) (sender.Instance, error) {
	v, ok := conf[Name]
	if !ok {
		return nil, fmt.Errorf("kafka sender instance: config not found")
	}
	c := Config{}
	if err := mapstructure.Decode(v, &c); err != nil {
		return nil, fmt.Errorf("kafka sender instance: decode error: %s", err)
	}
	if !c.Enabled {
		logger.Warnf("kafka sender instance: not enabled")
		return nil, nil
	}
	if len(c.Brokers) == 0 {
		return nil, fmt.Errorf("kafka sender instance: no brokers configured")
	}
	// 接收消息的 chan
	var ch chan sender.Msg
	if c.Buffer != 0 {
		ch = make(chan sender.Msg, c.Buffer)
	} else {
		ch = make(chan sender.Msg, buffer)
	}
	var ack sarama.RequiredAcks
	if c.AckType != "" {
		if v, ok := ackMap[strings.ToLower(c.AckType)]; ok {
			ack = v
		}
	} else {
		ack = sarama.WaitForAll
	}

	kafkaConf := sarama.NewConfig()
	if c.Retry != 0 {
		kafkaConf.Producer.Retry.Max = c.Retry
	} else {
		kafkaConf.Producer.Retry.Max = 3
	}

	kafkaConf.Producer.RequiredAcks = ack
	kafkaConf.Producer.Return.Successes = true
	client, err := sarama.NewSyncProducer(c.Brokers, kafkaConf)
	if err != nil {
		return nil, fmt.Errorf("kafka sender instance: create kafka client error: %s", err)
	}
	return &KafkaSender{
		c:      c,
		state:  false,
		client: client,
		mu:     sync.RWMutex{},
		wg:     sync.WaitGroup{},
		ch:     ch,
	}, nil
}

func (k *KafkaSender) GetName() string {
	return Name
}

func (k *KafkaSender) Alive() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.state
}

func (k *KafkaSender) turnOn() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.state {
		k.state = true
	}
}

func (k *KafkaSender) turnOff() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.state {
		k.state = false
	}
}

func (k *KafkaSender) RunSender(parent context.Context) error {
	k.turnOn()
	k.ctx, k.cancel = context.WithCancel(parent)

	var workerNum int
	if k.c.Worker != 0 {
		workerNum = k.c.Worker
	} else {
		workerNum = 3
	}

	for i := 0; i < workerNum; i++ {
		k.wg.Add(1)
		go k.consume(i)
	}
	return nil
}

func (k *KafkaSender) consume(idx int) {
	defer func() {
		k.wg.Done()
		k.turnOff()
	}()

	for {
		select {
		case msg, ok := <-k.ch:
			if !ok {
				logger.Warnf("kafka sender instance: channel closed, consumer goroutine %d exit", idx)
				return
			}
			topic, ok := msg.Where().(string)
			if !ok {
				logger.Errorf("kafka sender instance: transform origin topic error, origin topic: %+v", msg.Where())
				logger.Debugf("kafka sender instance: transform topic error, topic: %+v, drop data: %s", msg.Where(), msg.GetData())
				continue
			}
			data := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.ByteEncoder(msg.GetData()),
			}
			partition, offset, err := k.client.SendMessage(data)
			if err != nil {
				logger.Errorf("kafka sender instance: sender msg failed, error: %s", err)
				continue
			}
			logger.Debugf("kafka sender instance: send message to partition: %d, offset: %d, data: %s", partition, offset, msg.GetData())
		case <-k.ctx.Done():
			logger.Infof("kafka sender instance: context done, consumer goroutine %d exit", idx)
			return
		}
	}
}

func (k *KafkaSender) StopSender() {
	logger.Infoln("kafka sender instance: start to stop sender...")
	k.cancel()
	k.wg.Wait()
	close(k.ch)
	logger.Infoln("kafka sender instance: stop success")
}

func (k *KafkaSender) Push(msg sender.Msg) {
	k.ch <- msg
}
