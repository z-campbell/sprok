package sprok

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
)

// Client is a convenience wrapper around a Hub that tracks a set of topic subscriptions
// on behalf of a single logical consumer.
type Client[T MessageBase] struct {
	id            uuid.UUID
	hub           *Hub[T]
	ctx           context.Context
	cancel        context.CancelFunc
	subscriptions []*Subscriber[T]
}

// SprokClient describes the public surface of a Client: its identity and active subscriptions.
type SprokClient[T MessageBase] interface {
	GetId() uuid.UUID
	GetSubscriptions() []*Subscriber[T]
}

// IsConnected reports whether the client is connected; this implementation is currently a stub.
func (c *Client[T]) IsConnected() (string, bool) {
	return "", false
}

// NewClient creates a client wrapper around a hub with a fresh cancelable context.
func NewClient[T MessageBase](hub *Hub[T]) *Client[T] {
	ctx, cancel := context.WithCancel(context.Background())
	return &Client[T]{hub: hub, ctx: ctx, cancel: cancel, id: uuid.New()}
}

// GetId returns the client's unique identifier.
func (c *Client[T]) GetId() uuid.UUID {
	return c.id
}

// GetSubscriptions returns the list of topics the client is currently subscribed to.
func (c *Client[T]) GetSubscriptions() []*Subscriber[T] {
	return c.subscriptions
}

// Subscribe registers a new subscriber with the hub for the given topic.
func (c *Client[T]) Subscribe(topic string) error {
	s := NewSubscriber[T](topic, 10)
	err := c.hub.Subscribe(&s)
	if err != nil {
		return fmt.Errorf("unable to register subscriber for topic: %s. %w", topic, err)
	}
	c.subscriptions = append(c.subscriptions, &s)
	return nil
}

// Unsubscribe removes the client's subscription for a topic from the hub and from its local list.
func (c *Client[T]) Unsubscribe(topic string) error {
	for i, sub := range c.subscriptions {
		if sub.Topic == topic {
			if err := c.hub.Unsubscribe(*sub); err != nil {
				return fmt.Errorf("unable to unsubscribe from topic: %s. %w", topic, err)
			}
			sub.Close()
			c.subscriptions = append(c.subscriptions[:i], c.subscriptions[i+1:]...)
			return nil
		}
	}
	return errors.New("unable to unsubscribe from topic, topic not found")
}

// Close cancels the client context, stops its subscriptions, and removes them from the hub.
func (c *Client[T]) Close() {
	c.cancel()
	for _, sub := range c.subscriptions {
		sub.Close()
	}
	c.hub.RemoveSubscribers(c.subscriptions)
}
