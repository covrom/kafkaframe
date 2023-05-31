package main

import (
	"os"

	"github.com/covrom/kafkaframe"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Kafka kafkaframe.Config `yaml:"kafka"`
}

func (c *Config) Parse(fileName string) error {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return err
	}

	return nil
}
