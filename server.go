package sprok

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

// Broker describes the read-only surface of a Hub: enumerating subscribers/topics and shutting down.
type Broker[T MessageBase] interface {
	GetSubscribers() []*Subscriber[T]
	GetTopics() []string
	Close()
}

// Hub routes messages of type T between subscribers, grouped by topic, using a background
// worker pool. Create one with NewEventHub and register consumers with Subscribe.
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

// Close shuts down the hub, cancels subscribers, waits for workers to exit, and closes the internal queue.
func (h *Hub[T]) Close() {
	h.cancel()
	for _, subscriber := range h.Subscribers {
		subscriber.Close()
	}
	h.wg.Wait()
	h.Hub.Close()
}

// GetSubscribers returns the current subscriber list for the hub.
func (h *Hub[T]) GetSubscribers() []*Subscriber[T] {
	return h.Subscribers
}

// GetTopics returns the topic names currently known to the hub.
func (h *Hub[T]) GetTopics() []string {
	keys := make([]string, 0, len(h.Topics))
	for k := range h.Topics {
		keys = append(keys, k)
	}
	return keys
}

// NewEventHub creates a hub whose context derives from parentCtx and default internal channels.
func NewEventHub[T MessageBase](parentCtx context.Context) *Hub[T] {
	c, can := context.WithCancel(parentCtx)
	h := &Hub[T]{
		Hub:    CreateBufferedChannel[T](DefaultHubWorkerCount, DefaultHubBufferSize),
		ctx:    c,
		cancel: can,
		Topics: make(map[string][]*Subscriber[T]),
		Errors: CreateBufferedChannel[Message](DefaultErrorWorkerCount, DefaulErrorBufferSize),
	}
	return h
}

// Subscribe registers a subscriber with the hub and adds it to the topic routing table.
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

// Unsubscribe removes a subscriber from the hub's subscriber list and topic index.
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
					h.Topics[s.Topic] = append(h.Topics[s.Topic][:j], h.Topics[s.Topic][j+1:]...)
					if len(h.Topics[s.Topic]) == 0 {
						delete(h.Topics, s.Topic)
					}
					break
				}
			}
			return nil
		}
	}
	return fmt.Errorf("SubscriberId: %s not exists", s.SubscriberId)
}

// Publish queues a message for the hub workers to route to subscribers.
func (h *Hub[T]) Publish(e T) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	h.Hub.Add(e)
	return nil
}

// Start launches the hub's routing workers so published messages can be processed.
func (h *Hub[T]) Start(workers int) error {
	h.wg.Add(workers)

	for range workers {
		go h.runWorker(h.ctx, &h.wg)
	}

	return nil
}

// runWorker consumes messages from the hub queue and routes them to subscribers.
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

// RouteMessage fans a message to wildcard subscribers and to subscribers matching the message destination.
func (h *Hub[T]) RouteMessage(msg T) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	if msg.GetType() == Request {
		err := h.RouteRequest(msg)
		if err != nil {
			return fmt.Errorf("unable to route request: %w", err)
		}
	}

	// if destination/subject == "*" route to all subscribers, regardless of topic
	if msg.GetDestination() == "*" {
		for _, subscriber := range h.GetSubscribers() {
			subscriber.Channel.Add(msg)
		}
		return nil
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
// RouteRequest delivers a request message directly to the intended subscriber when it exists.
func (h *Hub[T]) RouteRequest(e T) error {
	for _, subscriber := range h.GetSubscribers() {
		if subscriber.SubscriberId.String() == e.GetDestination() {
			subscriber.Channel.Add(e)
			return nil
		}
	}
	return fmt.Errorf("SubscriberId: %s not exists", e.GetDestination())
}

// RemoveSubscribers removes a set of subscribers from the hub's subscriber list and topic routing table.
// It delegates to Unsubscribe for each subscriber so the two removal paths stay in sync.
func (h *Hub[T]) RemoveSubscribers(subs []*Subscriber[T]) {
	for _, sub := range subs {
		_ = h.Unsubscribe(*sub)
	}
}
