package etl

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/covrom/kafkaframe"
	"github.com/rs/xid"
)

type FullAndDeltaLoader interface {
	fmt.Stringer
	FullSync(ctx context.Context, taskId xid.ID, chbun chan SyncObjer) time.Time
}

type ExecuteContext struct {
	Kfk    *kafkaframe.Kafka
	Loader FullAndDeltaLoader
}

func (s ExecuteContext) ExecuteSync(ctx context.Context) error {
	taskId := xid.New()

	chsobj := make(chan SyncObjer, 10)
	wgsave := &sync.WaitGroup{}

	wgsave.Add(1)
	go sendKafka(ctx, s.Kfk, chsobj, wgsave)

	s.Loader.FullSync(ctx, taskId, chsobj)

	close(chsobj)
	wgsave.Wait()

	return nil
}
