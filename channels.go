package sprok

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

// SprokChannel describes a channel that supports reading with a timeout.
type SprokChannel[T any] interface {
	TryReadChannel(time.Duration) (T, error)
}

// BufferedChannel wraps a channel with an unbounded overflow buffer and a background
// worker pool that drains the buffer into the channel as space becomes available.
type BufferedChannel[T any] struct {
	Ch      chan T
	Buffer  *[]T
	mu      sync.Mutex
	cond    *sync.Cond
	running bool
	ctx     context.Context
	cancel  context.CancelFunc
}

// Start enables the buffered channel's background worker loop.
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

// Stop disables the buffered channel's background worker loop.
func (b *BufferedChannel[T]) Stop() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.running {
		return errors.New("channel is already stopped")
	}
	b.running = false
	b.cancel()
	b.cond.Broadcast()
	return nil
}

// CreateBufferedChannel allocates a buffered channel with a background worker pool and a backing buffer.
func CreateBufferedChannel[T any](workerCount int, bufferSize int) *BufferedChannel[T] {
	ctx, cancel := context.WithCancel(context.Background())
	b := &BufferedChannel[T]{
		Ch:     make(chan T, bufferSize),
		Buffer: &[]T{},
		cancel: cancel,
		ctx:    ctx,
	}
	b.cond = sync.NewCond(&b.mu)
	b.launchWorkers(ctx, workerCount)
	return b
}

// launchWorkers starts the background goroutines that move values from the buffer into the channel.
func (b *BufferedChannel[T]) launchWorkers(ctx context.Context, numWorkers int) {
	for range numWorkers {
		go b.runWorker(ctx)
	}
}

// Add queues a value into the buffered channel, using the in-memory channel when possible and the buffer otherwise.
func (b *BufferedChannel[T]) Add(t T) {
	select {
	case <-b.ctx.Done():
		b.cond.Broadcast()
		return
	case b.Ch <- t:
		b.cond.Broadcast()
	default:
		b.mu.Lock()
		*b.Buffer = append(*b.Buffer, t)
		b.mu.Unlock()
		b.cond.Broadcast()
	}
}

// TryReadChannel attempts to read a value from the channel within the requested timeout.
func (b *BufferedChannel[T]) TryReadChannel(timeout time.Duration) (*T, error) {
	var val T
	select {
	case val = <-b.Ch:
		return &val, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timeout")
	}
}

// runWorker moves values from the backing buffer into the main channel while the channel remains active.
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

// Close cancels the channel workers and closes the internal channel.
func (b *BufferedChannel[T]) Close() {
	b.cancel()
	close(b.Ch)
}
