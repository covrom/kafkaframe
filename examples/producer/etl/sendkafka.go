package etl

import (
	"context"
	"sync"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/producer/kafkaemployees"
)

type SyncObjer interface {
	HREvent() kafkaemployees.ProtoEvent
	Len() int
	TopicName() string
}

func sendKafka(ctx context.Context, kfk *kafkaframe.Kafka, ch chan SyncObjer,
	wg *sync.WaitGroup) {
	defer wg.Done()
	defer func() {
		for range ch {
			// drain
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case val, ok := <-ch:
			if !ok {
				return
			}

			evt := val.HREvent()

			tp, err := kfk.OpenTopic(val.TopicName())
			if err != nil {
				continue
			}
			if _, err := tp.SendProtobuf(context.Background(), evt); err != nil {
				tp.Close()
				continue
			}
			tp.Close()
		}
	}
}
