package main

func (h *Hub[Event]) SendEvent(e Event) {
	_ = h.Publish(e)
}
