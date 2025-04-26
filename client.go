package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
)

type Client[T MessageBase] struct {
	id            uuid.UUID
	hub           *Hub[T]
	ctx           context.Context
	cancel        context.CancelFunc
	subscriptions []*Subscriber[T]
}

type SprokClient[T MessageBase] interface {
	GetId() uuid.UUID
	GetSubscriptions() []*Subscriber[T]
}

func NewClient[T MessageBase](hub *Hub[T], ctx context.Context) *Client[T] {
	return &Client[T]{hub: hub, ctx: ctx, id: uuid.New()}
}

func (c *Client[T]) GetId() uuid.UUID {
	return c.id
}

func (c *Client[T]) GetSubscriptions() []*Subscriber[T] {
	return c.subscriptions
}
func (c *Client[T]) Subscribe(topic string) error {
	s := NewSubscriber[T](topic, 10)
	err := c.hub.Subscribe(&s)
	if err != nil {
		return fmt.Errorf("unable to register subscriber for topic: %s. %w", topic, err)
	}
	c.subscriptions = append(c.subscriptions, &s)
	return nil
}

func (c *Client[T]) Unsubscribe(topic string) error {
	for i, sub := range c.subscriptions {
		if sub.Topic == topic {
			c.subscriptions = append(c.subscriptions[:i], c.subscriptions[i+1:]...)
			return nil
		}
	}
	return errors.New("unable to unsubscribe from topic, topic not found")
}

func (c *Client[T]) Close() {
	c.cancel()
	for _, sub := range c.subscriptions {
		sub.cancel()
	}
}
