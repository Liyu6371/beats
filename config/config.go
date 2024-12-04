package config

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type Task map[string]interface{}

func (t Task) ToMap() map[string]interface{} {
	return t
}

func (t Task) Keys() []string {
	var keys []string
	for k, _ := range t {
		keys = append(keys, k)
	}
	return keys
}

func (t Task) Exist(key string) bool {
	if _, ok := t[key]; ok {
		return true
	}
	return false
}

func (t Task) GetSource() []string {
	var source []string
	v, ok := t["source"]
	if !ok {
		return source
	}
	for _, val := range v.([]interface{}) {
		source = append(source, val.(string))
	}
	return source
}

func (t Task) GetSender() []string {
	var sender []string
	v, ok := t["sender"]
	if !ok {
		return sender
	}
	for _, value := range v.([]interface{}) {
		sender = append(sender, value.(string))
	}
	return sender
}

type PeriodTask map[string]Task

func (p PeriodTask) TaskNames() []string {
	var names []string
	for k, _ := range p {
		names = append(names, k)
	}
	return names
}

func (p PeriodTask) GetTaskByName(name string) Task {
	return p[name]
}

type NonPeriodTask map[string]Task

func (np NonPeriodTask) TaskNames() []string {
	var names []string
	for k, _ := range np {
		names = append(names, k)
	}
	return names
}

func (np NonPeriodTask) GetTaskByName(name string) Task {
	return np[name]
}

var g *Config

type Config struct {
	TestModel     bool                   `yaml:"test_model"`
	Logger        Logger                 `yaml:"logger"`
	Sender        map[string]interface{} `yaml:"sender"`
	Source        map[string]interface{} `yaml:"source"`
	PeriodTask    PeriodTask             `yaml:"period_tasks"`
	NonPeriodTask NonPeriodTask          `yaml:"non_period_tasks"`
}

type Logger struct {
	Level string `yaml:"level"`
	Path  string `yaml:"path"`
}

func InitConfig(p string) error {
	if p == "" {
		return errors.New("config path shouldn't be empty")
	}
	dir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("unable to get wd, error: %s", err)
	}
	cPath := filepath.Join(dir, p)
	content, err := os.ReadFile(cPath)
	if err != nil {
		return fmt.Errorf("cannot read config file: %s, error: %v", cPath, err)
	}
	if err = yaml.Unmarshal(content, &g); err != nil {
		return fmt.Errorf("yaml unmarshal error: %v", err)
	}
	return nil
}

func GetConf() *Config {
	return g
}

func GetPeriodConf() PeriodTask {
	return g.PeriodTask
}

func GetNonPeriodConf() NonPeriodTask {
	return g.NonPeriodTask
}
