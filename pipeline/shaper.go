package pipeline

import "beats/sender"

var shaperFactory = map[string]func() Shaper{}

type Shaper interface {
	GetName() string
	Shape(interface{}) (sender.Msg, error)
}

func RegisterShaper(name string, fn func() Shaper) {
	shaperFactory[name] = fn
}

func Shapers() []string {
	var shapers []string
	for name := range shaperFactory {
		shapers = append(shapers, name)
	}
	return shapers
}
