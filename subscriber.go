package main

import "github.com/google/uuid"

type GenericSubscriber[T any] interface {
	GetId() uuid.UUID
	GetTopic() string
	TryReadChan() (T, bool)
}

type Subscriber[T any] struct {
	SubscriberId uuid.UUID
	Topic        string
	Ch           chan T
}

func (s Subscriber[T]) GetId() uuid.UUID {
	return s.SubscriberId
}

func (s Subscriber[T]) GetTopic() string {
	return s.Topic
}

func (s Subscriber[T]) TryReadChan() (T, bool) {
	select {
	case msg := <-s.Ch:
		return msg, true
	default:
		var msg T
		return msg, false
	}
}

func NewSubscriber[T any](topic string, maxBuff int) Subscriber[T] {
	return Subscriber[T]{
		SubscriberId: uuid.New(),
		Topic:        topic,
		Ch:           make(chan T, maxBuff)}
}
