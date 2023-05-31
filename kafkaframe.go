package kafkaframe

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/rs/xid"
	"gocloud.dev/pubsub/kafkapubsub"
)

type Kafka struct {
	Config    Config
	SaramaCfg *sarama.Config
}

func NewKafka(cfg Config) *Kafka {
	ret := &Kafka{
		Config:    cfg,
		SaramaCfg: kafkapubsub.MinimalConfig(),
	}
	ret.SaramaCfg.ClientID = fmt.Sprintf("%s_%s", cfg.ClientIDPrefix, xid.New().String())
	ret.SaramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return ret
}
