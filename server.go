package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const (
	DefaultHubBufferSize    = 1_000
	DefaulErrorBufferSize   = 100
	DefaultHubWorkerCount   = 7
	DefaultErrorWorkerCount = 3
)

// TODO: We need some method to clean up closed clients.
type Broker[T MessageBase] interface {
	GetSubscribers() []*Subscriber[T]
	GetTopics() []string
	Close()
}

type Hub[T MessageBase] struct {
	Hub         *BufferedChannel[T]
	ctx         context.Context
	cancel      context.CancelFunc
	mut         sync.Mutex
	Subscribers []*Subscriber[T]
	Topics      map[string][]*Subscriber[T] // "TopicName" : []Subscriber
	Errors      *BufferedChannel[Message]
	wg          sync.WaitGroup
}

func (h *Hub[T]) Close() {
	h.cancel()
	for _, subscriber := range h.Subscribers {
		subscriber.cancel()
	}
	h.wg.Wait()
	h.Hub.Close()
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
	c, can := context.WithCancel(context.Background())
	h := &Hub[T]{
		Hub:    CreateBufferedChannel[T](DefaultHubWorkerCount, DefaultHubBufferSize),
		ctx:    c,
		cancel: can,
		Topics: make(map[string][]*Subscriber[T]),
		Errors: CreateBufferedChannel[Message](DefaultErrorWorkerCount, DefaulErrorBufferSize),
	}
	return h
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
					if len(h.Topics[s.Topic]) == 0 {
						delete(h.Topics, s.Topic)
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

func (h *Hub[T]) Start(workers int) error {
	h.wg.Add(workers)

	for range workers {
		go h.runWorker(h.ctx, &h.wg)
	}

	if len(h.Errors.Ch) == 0 {
		return nil
	}
	return errors.New("error in starting workers")
}

func (h *Hub[T]) runWorker(ctx context.Context, wg *sync.WaitGroup) {
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			return
		case e := <-h.Hub.Ch:
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

	// if destination/subject == "*" route to all subs
	if msg.GetDestination() == "*" {

	}

	// Send to all subscribers on "*" default topic.
	for _, subscriber := range h.Topics["*"] {
		subscriber.Channel.Add(msg)
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

// TODO: Test this, and combine with unsubscribe.
func (h *Hub[T]) RemoveSubscribers(subs []*Subscriber[T]) {
	for _, sub := range subs {
		if _, ok := h.Topics[sub.Topic]; ok {
			for i, subscriber := range h.Topics[sub.Topic] {
				if subscriber.SubscriberId == sub.SubscriberId {
					h.Topics[sub.Topic] = append(h.Topics[sub.Topic][:i], h.Topics[sub.Topic][i+1:]...)
				}
				if len(h.Topics[sub.Topic]) == 0 {
					delete(h.Topics, sub.Topic)
				}
			}
		}
	}
}

func (h *Hub[T]) RouteError(e T) error {
	return nil
}
