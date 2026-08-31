package sprok

// SendEvent publishes a single event through the hub.
func (h *Hub[Event]) SendEvent(e Event) {
	_ = h.Publish(e)
}
