package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const (
	DefaultHubBufferSize    = 100_000
	DefaulErrorBufferSize   = 1000
	DefaultHubWorkerCount   = 10
	DefaultErrorWorkerCount = 10
)

type MessageHub[T MessageBase] interface {
	GetSubscribers() []*Subscriber[T]
	GetTopics() []string
}

type Hub[T MessageBase] struct {
	Hub         *BufferedChannel[T]
	ctx         context.Context
	cancel      context.CancelFunc
	mut         sync.Mutex
	Subscribers []*Subscriber[T]
	Topics      map[string][]*Subscriber[T] // "TopicName" : []Subscriber
	Errors      *BufferedChannel[Message]
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

func NewEventHub[T MessageBase](parentCtx context.Context) *Hub[T] {
	c, can := context.WithCancel(parentCtx)
	return &Hub[T]{
		Hub:    CreateBufferedChannel[T](parentCtx, DefaultHubWorkerCount, DefaultHubBufferSize),
		ctx:    c,
		cancel: can,
		Topics: make(map[string][]*Subscriber[T]),
		Errors: CreateBufferedChannel[Message](parentCtx, DefaultErrorWorkerCount, DefaulErrorBufferSize)}
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
			// Is there a better way? Yes, but only if order doesn't matter:
			// s[i] = s[len(s)-1] // move last item into the removed spot
			// s = s[:len(s)-1]   // truncate slice
			h.Subscribers = append(h.Subscribers[:i], h.Subscribers[i+1:]...)

			for j, sub := range h.Topics[s.Topic] {
				if sub.SubscriberId == s.SubscriberId {
					if len(h.Topics[s.Topic]) < 1 {
						//remove topic from map
					}
					h.Topics[s.Topic] = append(h.Topics[s.Topic][:j], h.Topics[s.Topic][j+1:]...)
				}
			}
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

func (h *Hub[T]) Start(workers int, ctx context.Context) error {

	for range workers {
		go h.runWorker(ctx)
	}

	if len(h.Errors.Ch) == 0 {
		return nil
	}
	return errors.New("error in starting workers")
}

func (h *Hub[T]) Stop() error {
	h.cancel()
	return nil
}

func (h *Hub[T]) runWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			e := <-h.Hub.Ch
			err := h.RouteMessage(e)
			if err != nil {
				h.Errors.Add(*NewErrorMessage(err, "HubWorker"))
			}
		}
	}
}

func (h *Hub[T]) hubWorker() error {
	return nil
}

func (h *Hub[T]) RouteMessage(msg T) error {
	h.mut.Lock()
	defer h.mut.Unlock()

	if msg.GetType() == Request {
		err := h.RouteRequest(msg)
		if err != nil {
			return fmt.Errorf("unable to route request: %w", err)
		}
	}
	if _, ok := h.Topics[msg.GetDestination()]; !ok {
		return errors.New("Invalid destination for event: " + msg.GetDestination())
	}
	for _, s := range h.Topics[msg.GetDestination()] {
		s.Channel.Add(msg)
	}
	return nil
}

// Routes requests directly to subscribers
func (h *Hub[T]) RouteRequest(e T) error {
	for _, subscriber := range h.GetSubscribers() {
		if subscriber.SubscriberId.String() == e.GetDestination() {
			subscriber.Channel.Add(e)
			return nil
		}
	}
	return fmt.Errorf("SubscriberId: %s not exists", e.GetDestination())
}

func (h *Hub[T]) RouteError(e T) error {
	return nil
}
