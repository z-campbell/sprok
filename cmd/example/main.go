// Command example demonstrates the minimal pub/sub workflow provided by sprok:
// creating a hub, subscribing to a topic, publishing a message, and reading it back.
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/z-campbell/sprok"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a hub for Message payloads and start its routing workers.
	hub := sprok.NewEventHub[sprok.Message](ctx)
	if err := hub.Start(sprok.DefaultHubWorkerCount); err != nil {
		fmt.Println("failed to start hub:", err)
		return
	}
	defer hub.Close()

	// Subscribe to the "greetings" topic.
	sub := sprok.NewSubscriber[sprok.Message]("greetings", 10)
	if err := hub.Subscribe(&sub); err != nil {
		fmt.Println("failed to subscribe:", err)
		return
	}

	// Publish a message to that topic.
	msg, err := sprok.NewMessage(sprok.Event, "example", "greetings", "hello, sprok!")
	if err != nil {
		fmt.Println("failed to create message:", err)
		return
	}
	if err := hub.Publish(*msg); err != nil {
		fmt.Println("failed to publish:", err)
		return
	}

	// Read the message back from the subscriber's channel.
	received, err := sub.Channel.TryReadChannel(time.Second)
	if err != nil {
		fmt.Println("failed to read message:", err)
		return
	}

	var payload string
	if err := received.Unmarshal(&payload); err != nil {
		fmt.Println("failed to decode payload:", err)
		return
	}

	fmt.Printf("received on %q from %q: %s\n", received.Destination, received.Source, payload)
}
