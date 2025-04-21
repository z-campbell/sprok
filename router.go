package main

type MessageRouter[T MessageBase] interface {
	RouteRequest(T) error
	RouteFanout(T) error
}

type Router[T MessageBase] struct {
}

func (r *Router[T]) RouteRequest(msg T) error {
	return nil
}

func (r *Router[T]) RouteFanout(msg T) error {
	return nil
}
