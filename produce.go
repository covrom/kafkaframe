package kafkaframe

import (
	"context"
	"encoding/json"

	"github.com/rs/xid"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
	"google.golang.org/protobuf/proto"
)

type Topic struct {
	Topic       string
	PubsubTopic *pubsub.Topic
	k           *Kafka
}

func (k *Kafka) OpenTopic(topicName string) (*Topic, error) {
	pt, err := kafkapubsub.OpenTopic(k.Config.Brokers, k.SaramaCfg, topicName, &kafkapubsub.TopicOptions{
		KeyName: idField,
	})
	if err != nil {
		return nil, err
	}
	return &Topic{
		Topic:       topicName,
		PubsubTopic: pt,
		k:           k,
	}, nil
}

func (t *Topic) Close() error {
	return t.PubsubTopic.Shutdown(context.Background())
}

func (t *Topic) SendJSON(ctx context.Context, v interface{}) (*MessageHeader, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	km := MessageHeader{
		ID:       xid.New().String(),
		Topic:    t.Topic,
		Source:   t.k.Config.ClientIDPrefix,
		Producer: t.k.SaramaCfg.ClientID,
	}
	if err := t.PubsubTopic.Send(ctx, &pubsub.Message{
		Body:     body,
		Metadata: km.toMap(),
	}); err != nil {
		return nil, err
	}
	return &km, nil
}

func (t *Topic) SendProtobuf(ctx context.Context, v proto.Message) (*MessageHeader, error) {
	body, err := proto.Marshal(v)
	if err != nil {
		return nil, err
	}
	km := MessageHeader{
		ID:       xid.New().String(),
		Topic:    t.Topic,
		Source:   t.k.Config.ClientIDPrefix,
		Producer: t.k.SaramaCfg.ClientID,
	}
	if err := t.PubsubTopic.Send(ctx, &pubsub.Message{
		Body:     body,
		Metadata: km.toMap(),
	}); err != nil {
		return nil, err
	}
	return &km, nil
}
