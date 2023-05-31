package kfkempl

import (
	"context"

	"time"

	"github.com/covrom/kafkaframe"
	"github.com/covrom/kafkaframe/examples/producer/kafkaemployees"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/protobuf/proto"
)

type EventMessageObjectType any

type EventMessage[T EventMessageObjectType] interface {
	*T
	proto.Message
	GetEvent() *kafkaemployees.Event
	Len() int
}

type SimpleSubscribe[OTM, OTD EventMessageObjectType, TM EventMessage[OTM], TD EventMessage[OTD], M any] struct {
	Kfk *kafkaframe.Kafka
	CntModify,
	CntDelete,
	CntErrors prometheus.Counter
	DurSec          prometheus.Histogram
	TopicModify     string
	TopicDelete     string
	ProcessModify   func(t TM) ([]M, error)
	GetIdsForDelete func(t TD) ([]string, error)
}

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) Serve(ctx context.Context) {
	go s.workerModify(ctx)
	go s.workerDelete(ctx)
}

// MODIFY

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) workerModify(ctx context.Context) {
	ts, err := s.Kfk.OpenSubscription(SubscribeGroup, s.TopicModify)
	if err != nil {
		return
	}
	defer ts.Close()

	for {
		done := make(chan struct{})
		go s.receiveModify(ctx, ts, done)
		select {
		case <-ctx.Done():
			return
		case <-done:
		}
	}
}

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) receiveModify(ctx context.Context, ts *kafkaframe.Subscription, done chan struct{}) {
	defer close(done)

	tm := TM(new(OTM))

	msg, err := ts.ReceiveProtobuf(ctx, tm)
	if err != nil {
		s.CntErrors.Inc()
		return
	}

	msg.Ack()

	tstart := time.Now()
	s.modify(ctx, tm)
	s.DurSec.Observe(float64(time.Since(tstart)) / float64(time.Second))
}

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) modify(ctx context.Context, tm TM) {

	vals, err := s.ProcessModify(tm)
	if err != nil {
		s.CntErrors.Inc()
		return
	}

	if len(vals) > 0 {
		// here you must store vals into database
	}
	s.CntModify.Add(float64(len(vals)))
}

// DELETE

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) workerDelete(ctx context.Context) {
	ts, err := s.Kfk.OpenSubscription(SubscribeGroup, s.TopicDelete)
	if err != nil {
		s.CntErrors.Inc()
		return
	}
	defer ts.Close()

	for {
		done := make(chan struct{})
		go s.receiveDelete(ctx, ts, done)
		select {
		case <-ctx.Done():
			return
		case <-done:
		}
	}
}

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) receiveDelete(ctx context.Context, ts *kafkaframe.Subscription, done chan struct{}) {
	defer close(done)

	td := TD(new(OTD))

	msg, err := ts.ReceiveProtobuf(ctx, td)
	if err != nil {
		s.CntErrors.Inc()
		return
	}

	msg.Ack()

	tstart := time.Now()
	s.delete(ctx, td)
	s.DurSec.Observe(float64(time.Since(tstart)) / float64(time.Second))
}

func (s *SimpleSubscribe[OTM, OTD, TM, TD, M]) delete(ctx context.Context, tm TD) {
	ids, err := s.GetIdsForDelete(tm)
	if err != nil {
		s.CntErrors.Inc()

		return
	}

	// here you must delete items from database

	s.CntDelete.Add(float64(len(ids)))
}
