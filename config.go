package kafkaframe

type Config struct {
	ClientIDPrefix string   `yaml:"clientIdPrefix"`
	Brokers        []string `yaml:"brokers"`
}
