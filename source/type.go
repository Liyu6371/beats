package source

import "context"

type Instance interface {
	GetName() string
	StartSource(context.Context, chan<- []byte) error
	Alive() bool
	Stop()
}

var instanceFactory map[string]func(map[string]interface{}) (Instance, error)

func RegisterSource(name string, fn func(map[string]interface{}) (Instance, error)) {
	instanceFactory[name] = fn
}

func GetSources() []string {
	var instance []string
	for name := range instanceFactory {
		instance = append(instance, name)
	}
	return instance
}
