package kafka

import "github.com/Shopify/sarama"

var (
	//rebalance         = sarama.BalanceStrategyRange
	consumerGroup     = "kafka_default_consumer_group"
	kafkaRebalanceMap = map[string]sarama.BalanceStrategy{
		"sticky":     sarama.BalanceStrategySticky,
		"roundrobin": sarama.BalanceStrategyRoundRobin,
		"range":      sarama.BalanceStrategyRange,
	}
)

type Config struct {
	Enabled       bool     `mapstructure:"enabled"`
	Host          string   `mapstructure:"host"`
	Port          int      `mapstructure:"port"`
	Username      string   `mapstructure:"username"`
	Password      string   `mapstructure:"password"`
	Version       string   `mapstructure:"version"`
	ConsumerGroup string   `mapstructure:"kafka_consumer_group"`
	ConsumeOldest bool     `mapstructure:"kafka_oldest"`
	Assignor      string   `mapstructure:"kafka_assignor"`
	Topics        []string `mapstructure:"topics"`
}
