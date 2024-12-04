package sender

import (
	"context"
)

// Msg 抽象
type Msg interface {
	GetSender() string  // 数据需要发往的 sender 实例名称
	GetData() []byte    // 获取需要发送的数据
	Where() interface{} // 补充信息 可以是 string 也可以是数字类型
}

// Instance sender 实例接口
type Instance interface {
	Push(Msg)                  // 推送数据到 sender 的 channel 中
	RunSender(context.Context) // 启动 sender 实例
	StopSender()               // 停止 sender 实例
	Alive() bool               // 判定 sender 实例是否存活
	GetName() string           // 返回 sender 实例名称
}

var instanceFactory = map[string]func(map[string]interface{}) Instance{}

func RegisterInstance(name string, fn func(map[string]interface{}) Instance) {
	instanceFactory[name] = fn
}

// GetInstances 返回注册了的实例化 sender 的方法
func GetInstances() []string {
	var instance []string
	for name := range instanceFactory {
		instance = append(instance, name)
	}
	return instance
}
