package socket

import (
	"github.com/TencentBlueKing/bkmonitor-datalink/pkg/libgse/gse"
	"time"
)

var (
	buffer         = 500
	defaultGseConf = gse.Config{
		MsgQueueSize:   1,
		WriteTimeout:   5 * time.Second,
		ReadTimeout:    60 * time.Second,
		Nonblock:       false,
		RetryTimes:     3,
		RetryInterval:  3 * time.Second,
		ReconnectTimes: 3,
		Endpoint:       "/var/run/ipc.state.report",
	}
)

type Config struct {
	Enabled  bool   `mapstructure:"enabled"`
	Worker   int    `mapstructure:"worker"`
	Buffer   int    `mapstructure:"buffer"`
	EndPoint string `mapstructure:"end_point"`
}
