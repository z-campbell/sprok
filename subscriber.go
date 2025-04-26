package main

import (
	"context"
	"github.com/google/uuid"
)

const (
	defaultBufferSize  = 100
	defaultWorkerCount = 1
)

type GenericSubscriber[T any] interface {
	GetId() uuid.UUID
	GetTopic() string
	TryReadChan() (T, bool)
}

type Subscriber[T any] struct {
	SubscriberId uuid.UUID
	Topic        string
	Channel      *BufferedChannel[T]
	cancel       context.CancelFunc
}

func (s *Subscriber[T]) GetId() uuid.UUID {
	return s.SubscriberId
}

func (s *Subscriber[T]) GetTopic() string {
	return s.Topic
}

func NewSubscriber[T any](topic string, maxBuff int) Subscriber[T] {
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	return Subscriber[T]{
		SubscriberId: uuid.New(),
		Topic:        topic,
		Channel:      CreateBufferedChannel[T](cancelCtx, defaultWorkerCount, maxBuff),
		cancel:       cancelFunc,
	}
}

func (s *Subscriber[T]) Close() {

}
