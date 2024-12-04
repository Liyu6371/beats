package kafka

import (
	"beats/logger"
	"beats/source"
	"context"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mitchellh/mapstructure"
	"sync"
)

const Name = "kafka"

func init() {
	source.RegisterSource(Name, New)
}

type KafkaSource struct {
	c      Config
	state  bool
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	client  sarama.ConsumerGroup
	handler sarama.ConsumerGroupHandler
}

func New(conf map[string]interface{}) source.Instance {
	v, ok := conf[Name]
	if !ok {
		logger.Errorf("kafka source instance: config not found")
		return nil
	}
	c := Config{}
	if err := mapstructure.Decode(v, &c); err != nil {
		logger.Errorf("kafka source instance: decode error: %s", err)
		return nil
	}
	if !c.Enabled {
		logger.Warnf("kafka source instance: not enabled")
		return nil
	}
	saramaConf := sarama.NewConfig()
	if c.Version != "" {
		if v, err := sarama.ParseKafkaVersion(c.Version); err == nil {
			saramaConf.Version = v
		}
	}
	if c.Username != "" && c.Password != "" {
		saramaConf.Net.SASL.Enable = true
		saramaConf.Net.SASL.User = c.Username
		saramaConf.Net.SASL.Password = c.Password
	}
	if c.ConsumeOldest {
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		saramaConf.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	if v, ok := kafkaRebalanceMap[c.Assignor]; ok {
		saramaConf.Consumer.Group.Rebalance.Strategy = v
	} else {
		saramaConf.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	}

	var cg string
	if c.ConsumerGroup != "" {
		cg = c.ConsumerGroup
	} else {
		cg = consumerGroup
	}

	addr := fmt.Sprintf("%s:%d", c.Host, c.Port)
	client, err := sarama.NewConsumerGroup([]string{addr}, cg, saramaConf)
	if err != nil {
		logger.Errorf("kafka source instance: create consumer group client error: %s", err)
		return nil
	}
	return &KafkaSource{
		c:      c,
		state:  false,
		mu:     sync.RWMutex{},
		client: client,
	}
}

func (k *KafkaSource) GetName() string {
	return Name
}

func (k *KafkaSource) StartSource(parent context.Context, output chan<- []byte) {
	k.ctx, k.cancel = context.WithCancel(parent)
	k.handler = &Handler{ch: output}

	k.turnOn()
	defer func() {
		k.turnOff()
		if err := k.client.Close(); err != nil {
			logger.Errorf("kafka source instance: close client error: %s", err)
		}
	}()

	// 常驻进程，监听到 ctx.Done 后退出
	for {
		if err := k.client.Consume(k.ctx, k.c.Topics, k.handler); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				logger.Errorf("kafka source instance: error is : %s", err)
				return
			}
			logger.Errorf("kafka source instance: Error from consumer: %s", err)
			return
		}
		if k.ctx.Err() != nil {
			return
		}
	}

}

func (k *KafkaSource) Alive() bool {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.state
}

func (k *KafkaSource) turnOn() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.state {
		k.state = true
	}
}

func (k *KafkaSource) turnOff() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.state {
		k.state = false
	}
}

func (k *KafkaSource) Stop() {
	k.cancel()
	//k.client.Close()
}

type Handler struct {
	ch chan<- []byte
}

func (h *Handler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				logger.Infof("Source->%s message chan claim is closed.", Name)
				return nil
			}
			h.ch <- msg.Value
		case <-session.Context().Done():
			logger.Info("session exit")
			return nil
		}
	}
}
