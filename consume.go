package kafkaframe

import (
	"context"
	"encoding/json"
	"fmt"

	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/kafkapubsub"
	"google.golang.org/protobuf/proto"
)

type Subscription struct {
	PubsubSubscription *pubsub.Subscription
	k                  *Kafka
}

func (k *Kafka) OpenSubscription(group string, topics ...string) (*Subscription, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("topics is empty")
	}
	ps, err := kafkapubsub.OpenSubscription(k.Config.Brokers, k.SaramaCfg, group, topics, &kafkapubsub.SubscriptionOptions{
		KeyName: idField,
	})
	if err != nil {
		return nil, err
	}
	return &Subscription{
		PubsubSubscription: ps,
		k:                  k,
	}, nil
}

func (t *Subscription) Close() error {
	return t.PubsubSubscription.Shutdown(context.Background())
}

type ReceivedMessage struct {
	MessageHeader
	*pubsub.Message
}

// ReceiveJSON (blocking) and unmarshal from JSON a next message from subscription into 'dest' that must be a pointer.
// The Ack method of the returned ReceivedMessage must be called once the message has
// been processed, to prevent it from being received again.
func (t *Subscription) ReceiveJSON(ctx context.Context, dest interface{}) (*ReceivedMessage, error) {
	m, err := t.PubsubSubscription.Receive(ctx)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(m.Body, dest); err != nil {
		return nil, err
	}
	ret := &ReceivedMessage{
		Message: m,
	}
	(&ret.MessageHeader).fromMap(m.Metadata)
	return ret, nil
}

// ReceiveProtobuf (blocking) and unmarshal from proto a next message from subscription into 'dest' that must be a pointer.
// The Ack method of the returned ReceivedMessage must be called once the message has
// been processed, to prevent it from being received again.
func (t *Subscription) ReceiveProtobuf(ctx context.Context, dest proto.Message) (*ReceivedMessage, error) {
	m, err := t.PubsubSubscription.Receive(ctx)
	if err != nil {
		return nil, err
	}
	if err := proto.Unmarshal(m.Body, dest); err != nil {
		return nil, err
	}
	ret := &ReceivedMessage{
		Message: m,
	}
	(&ret.MessageHeader).fromMap(m.Metadata)
	return ret, nil
}
