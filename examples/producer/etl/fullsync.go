package etl

import (
	"context"
	"time"

	"github.com/rs/xid"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

type HRGCliModel interface {
	GetId() string
}

type HRModel interface {
	IsZero() bool
	GetUpdatedAt() *timestamppb.Timestamp
}

type FullSyncContext[T HRGCliModel, R HRModel] struct {
	Fetcher   func(ctx context.Context, ch chan []T)
	Convertor func(hrel T) R
	Builder   func(taskId xid.ID, els []R) SyncObjer
}

func (s FullSyncContext[T, R]) FullSync(ctx context.Context, taskId xid.ID, chout chan SyncObjer) time.Time {
	ch := make(chan []T, 100)
	go s.Fetcher(ctx, ch)

	var maxT time.Time
	for hrels := range ch {
		ids := make([]string, len(hrels))
		for i, hrel := range hrels {
			ids[i] = hrel.GetId()
		}

		retels := make([]R, 0, len(hrels))
		for _, hrel := range hrels {
			retel := s.Convertor(hrel)
			if retel.IsZero() {
				continue
			}
			if maxT.IsZero() || retel.GetUpdatedAt().AsTime().After(maxT) {
				maxT = retel.GetUpdatedAt().AsTime()
			}
			retels = append(retels, retel)
		}
		chout <- s.Builder(taskId, retels)
	}
	return maxT
}
