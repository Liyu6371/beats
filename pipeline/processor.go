package pipeline

var processorFactory = map[string]func() Processor{}

type Processor interface {
	GetName() string
	Process(interface{}) (interface{}, error)
}

func RegisterProcessor(name string, fn func() Processor) {
	processorFactory[name] = fn
}

func Processors() []string {
	var processors []string
	for name := range processorFactory {
		processors = append(processors, name)
	}
	return processors
}
