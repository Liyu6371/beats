package kafka

import (
	"github.com/Shopify/sarama"
)

var (
	buffer = 500
	ackMap = map[string]sarama.RequiredAcks{
		"all":   sarama.WaitForAll,
		"local": sarama.WaitForLocal,
		"no":    sarama.NoResponse,
	}
)

type Config struct {
	Enabled bool     `mapstructure:"enabled"`
	Worker  int      `mapstructure:"worker"`
	Buffer  int      `mapstructure:"buffer"`
	Retry   int      `mapstructure:"retry"`
	AckType string   `mapstructure:"ack_type"`
	Brokers []string `mapstructure:"brokers"`
}
