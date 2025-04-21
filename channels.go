package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	DefaultChannelBufferSize = 10
	DefaultDelayInterval     = 1 * time.Millisecond
)

type SprokChannel[T any] interface {
	TryReadChannel(time.Duration) (T, error)
}

type BufferedChannel[T any] struct {
	Ch      chan T
	Buffer  *[]T
	mu      sync.Mutex
	cond    *sync.Cond
	running bool
	ctx     context.Context
}

func (b *BufferedChannel[T]) Start(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.running {
		return errors.New("channel is already running")
	}
	go b.runChanHandler(ctx)
	return nil
}

func (b *BufferedChannel[T]) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.running {
		return errors.New("channel is already stopped")
	}
	b.running = false
	b.ctx.Done()
	return nil
}

func CreateBufferedChannel[T any]() *BufferedChannel[T] {
	b := &BufferedChannel[T]{
		Ch:     make(chan T, DefaultChannelBufferSize),
		Buffer: &[]T{},
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *BufferedChannel[T]) Add(t T) {
	select {
	case b.Ch <- t:
	default:
		b.mu.Lock()
		*b.Buffer = append(*b.Buffer, t)
		b.mu.Unlock()
		b.cond.Broadcast()
	}
}

func (b *BufferedChannel[T]) TryReadChannel(timeout time.Duration) (*T, error) {
	var val T
	select {
	case val = <-b.Ch:
		return &val, nil
	case <-time.After(timeout):
		return &val, fmt.Errorf("timeout")
	}
}

func (b *BufferedChannel[T]) runChanHandler(ctx context.Context) {
	for {
		b.mu.Lock()
		if len(*b.Buffer) == 0 {
			b.mu.Unlock()
			time.Sleep(DefaultDelayInterval) // avoid tight spinning
			continue
		}
		val := (*b.Buffer)[0]
		*b.Buffer = (*b.Buffer)[1:]
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return
		case b.Ch <- val:
		}
	}
}
