package main

import "sync"

// TODO: Create router pool. This will be used to shard the input buffer.

type MessageRouter[T MessageBase] interface {
	AddMessage(T) error
	RouteRequest(T) error
	RouteFanout(T) error
	Start() error
}

// TODO:
type RouterPool[T MessageBase] struct {
}

func NewRouterPool[T MessageBase]() RouterPool[T] {
	return RouterPool[T]{}
}

type Router[T MessageBase] struct {
	inputBuffer *BufferedChannel[T]
	mut         sync.Mutex
	wg          sync.WaitGroup
	WorkerCount int
}

func NewRouter[T MessageBase](inputBuffer *BufferedChannel[T]) *Router[T] {

	return &Router[T]{}
}

func (r *Router[T]) AddMessage(msg T) error {
	return nil
}

func (r *Router[T]) RouteRequest(msg T) error {
	return nil
}

func (r *Router[T]) RouteFanout(msg T) error {
	return nil
}
