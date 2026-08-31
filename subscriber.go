package sprok

import (
	"context"
	"github.com/google/uuid"
)

const (
	defaultBufferSize  = 100
	defaultWorkerCount = 1
)

// GenericSubscriber describes the public surface of a Subscriber: its identity, topic, and non-blocking read.
type GenericSubscriber[T any] interface {
	GetId() uuid.UUID
	GetTopic() string
	TryReadChan() (T, bool)
}

// Subscriber represents a single registration to a Hub topic, backed by its own buffered channel.
type Subscriber[T any] struct {
	SubscriberId uuid.UUID
	Topic        string
	Channel      *BufferedChannel[T]
	cancel       context.CancelFunc
	ctx          context.Context
}

// GetId returns the subscriber's unique identifier.
func (s *Subscriber[T]) GetId() uuid.UUID {
	return s.SubscriberId
}

// GetTopic returns the topic the subscriber is watching.
func (s *Subscriber[T]) GetTopic() string {
	return s.Topic
}

// NewSubscriber creates a subscriber with its own buffered channel and cancellation context.
func NewSubscriber[T any](topic string, maxBuff int) Subscriber[T] {
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	return Subscriber[T]{
		SubscriberId: uuid.New(),
		Topic:        topic,
		Channel:      CreateBufferedChannel[T](defaultWorkerCount, maxBuff),
		cancel:       cancelFunc,
		ctx:          cancelCtx,
	}
}

// Close cancels the subscriber and closes its buffered channel.
func (s *Subscriber[T]) Close() {
	s.cancel()
	s.Channel.Close()

}
