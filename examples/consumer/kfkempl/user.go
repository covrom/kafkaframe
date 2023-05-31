package kfkempl

import (
	"context"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/producer/kafkaemployees"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type DBUser struct {
}

type UsersSubscribe struct {
	s SimpleSubscribe[kafkaemployees.HRUsersModify,
		kafkaemployees.HRUsersDelete,
		*kafkaemployees.HRUsersModify,
		*kafkaemployees.HRUsersDelete,
		DBUser]
}

func NewUsersSubscribe(ctx context.Context, kfk *kafkaframe.Kafka) *UsersSubscribe {
	ret := &UsersSubscribe{
		s: SimpleSubscribe[kafkaemployees.HRUsersModify,
			kafkaemployees.HRUsersDelete,
			*kafkaemployees.HRUsersModify,
			*kafkaemployees.HRUsersDelete,
			DBUser]{
			Kfk:         kfk,
			CntModify:   cntModifyUsers,
			CntDelete:   cntDeleteUsers,
			CntErrors:   cntUsersErrors,
			DurSec:      dursecUsers,
			TopicModify: kafkaemployees.UsersModifyTopicName,
			TopicDelete: kafkaemployees.UsersDeleteTopicName,
		},
	}
	ret.s.ProcessModify = ret.processModify
	ret.s.GetIdsForDelete = ret.getDeleteIds

	ret.s.Serve(ctx)
	return ret
}

var (
	cntModifyUsers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "modified_users",
		Help: "The total number of modified users",
	})
	cntDeleteUsers = promauto.NewCounter(prometheus.CounterOpts{
		Name: "deleted_users",
		Help: "The total number of deleted users",
	})
	cntUsersErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "failed_users",
		Help: "The total number of failed users",
	})
	dursecUsers = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "sec_duration_users",
		Help:    "The duration of the users",
		Buckets: prometheus.ExponentialBucketsRange(0.1, 24*60*60, 18),
	})
)

// nolint
func (s *UsersSubscribe) processModify(tm *kafkaemployees.HRUsersModify) ([]DBUser, error) {
	els := tm.Users
	vals := make([]DBUser, 0, len(els))
	for _, v := range els {
		if v.Id == "" || v.CreatedAt.AsTime().IsZero() {
			continue
		}
		// here you must convert kafka user to database user
		// and add it to vals:
		// vals = append(vals, el)

	}
	return vals, nil
}

func (s *UsersSubscribe) getDeleteIds(t *kafkaemployees.HRUsersDelete) ([]string, error) {
	return t.UserIds, nil
}
