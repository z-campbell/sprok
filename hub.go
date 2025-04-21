package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
)

type MessageHub[T MessageBase] interface {
	GetSubscribers() []*Subscriber[T]
	GetTopics() []string
}

type Hub[T MessageBase] struct {
	Hub         BufferedChannel[T]
	Ctx         context.Context
	mut         sync.Mutex
	Subscribers []*Subscriber[T]
	Topics      map[string][]*Subscriber[T] // "TopicName" : []Subscriber
	Errors      BufferedChannel[ErrorMessage]
}

func (h *Hub[T]) GetSubscribers() []*Subscriber[T] {
	return h.Subscribers
}

func (h *Hub[T]) GetTopics() []string {
	keys := make([]string, 0, len(h.Topics))
	for k := range h.Topics {
		keys = append(keys, k)
	}
	return keys
}

func NewEventHub[T MessageBase](ctx context.Context) *Hub[T] {
	return &Hub[T]{
		Hub:    *CreateBufferedChannel[T](),
		Ctx:    ctx,
		Topics: make(map[string][]*Subscriber[T]),
		Errors: *CreateBufferedChannel[ErrorMessage]()}
}

func (h *Hub[T]) Subscribe(s *Subscriber[T]) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	for _, subscriber := range h.GetSubscribers() {
		if subscriber.SubscriberId == s.SubscriberId {
			return fmt.Errorf("SubscriberId %s already exists", s.SubscriberId)
		}
	}
	h.Subscribers = append(h.Subscribers, s)
	h.Topics[s.Topic] = append(h.Topics[s.Topic], s)
	return nil
}

func (h *Hub[T]) Unsubscribe(s Subscriber[T]) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	for i, subscriber := range h.Subscribers {
		if subscriber.SubscriberId == s.SubscriberId {
			h.Subscribers = append(h.Subscribers[:i], h.Subscribers[i+1:]...)
			return nil
		}
	}
	return fmt.Errorf("SubscriberId: %s not exists", s.SubscriberId)
}

func (h *Hub[T]) Publish(e T) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.Hub.Add(e)
	return nil
}

func (h *Hub[T]) Run(workers int, ctx context.Context) error {
	_ = strconv.Itoa(workers)
	slog.Info("Starting event hub")
	for {
		select {
		case <-h.Ctx.Done():
			slog.Info("Message hub stopped.")
			return context.Canceled
		default:
			e := <-h.Hub.Ch
			err := h.RouteMessage(e)
			if err != nil {
				return err
			}
		}
	}
}

func (h *Hub[T]) hubWorker() error {
	return nil
}

func (h *Hub[T]) RouteMessage(e T) error {
	h.mut.Lock()
	defer h.mut.Unlock()

	if e.GetType() == Request {
		err := h.RouteRequest(e)
		if err != nil {
			return fmt.Errorf("unable to route request: %w", err)
		}
	}
	if _, ok := h.Topics[e.GetDestination()]; !ok {
		return errors.New("Invalid destination for event: " + e.GetDestination())
	}
	for _, s := range h.Topics[e.GetDestination()] {
		s.Ch <- e
	}
	return nil
}

// Routes requests directly to subscribers
func (h *Hub[T]) RouteRequest(e T) error {
	for _, subscriber := range h.GetSubscribers() {
		if subscriber.SubscriberId.String() == e.GetDestination() {
			subscriber.Ch <- e
			return nil
		}
	}
	return fmt.Errorf("SubscriberId: %s not exists", e.GetDestination())
}

func (h *Hub[T]) RouteError(e T) error {
	return nil
}
