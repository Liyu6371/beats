package source

import "context"

// source 类型任务定义
// source 类型任务一般具有 常驻、数据流式传输的特征，例如 kafka topic 的监听
// 这一任务类型产生的数据是源源不断的、非周期性的，因此 StartSource 方法内部的实现往往是不会主动退出的
// 而是类似于 for{} 循环这种常驻的形式

type Instance interface {
	GetName() string
	StartSource(context.Context, chan<- []byte)
	Alive() bool
	Stop()
}

var instanceFactory = map[string]func(map[string]interface{}) Instance{}

func RegisterSource(name string, fn func(map[string]interface{}) Instance) {
	instanceFactory[name] = fn
}

func GetSources() []string {
	var instance []string
	for name := range instanceFactory {
		instance = append(instance, name)
	}
	return instance
}
