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
	b.running = true
	go b.runWorker(ctx)
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

func CreateBufferedChannel[T any](ctx context.Context, workerCount int, bufferSize int) *BufferedChannel[T] {
	b := &BufferedChannel[T]{
		Ch:     make(chan T, bufferSize),
		Buffer: &[]T{},
	}
	b.cond = sync.NewCond(&b.mu)
	b.launchWorkers(ctx, workerCount)
	return b
}

func (b *BufferedChannel[T]) launchWorkers(ctx context.Context, numWorkers int) {
	for range numWorkers {
		go b.runWorker(ctx)
	}
}

func (b *BufferedChannel[T]) Add(t T) {
	select {
	case b.Ch <- t:
		b.cond.Broadcast()
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
		return nil, fmt.Errorf("timeout")
	}
}

func (b *BufferedChannel[T]) runWorker(ctx context.Context) {
	for {
		b.mu.Lock()
		// add the check for the cond.
		for len(*b.Buffer) == 0 {
			if ctx.Err() != nil {
				b.mu.Unlock()
				return
			}
			b.cond.Wait()
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
