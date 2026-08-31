package sprok

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	numPublishers           = 100
	numSubscribers          = 10
	numMessagesPerPublisher = 10000
	numHubWorkers           = 5
	maxSubBuffer            = 100
)

// TODO: Parameterize this test rather than defining consts.
// TODO: Add in profiling and benchmarking.
// StressTest runs a synthetic load test against the hub by creating subscribers,
// spawning publishers, and measuring how quickly messages are delivered.
func StressTest() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	hub := NewEventHub[Message](ctx)
	err := hub.Start(numHubWorkers)
	start := time.Now()
	if err != nil {
		fmt.Printf("Error starting hub with numWorkers= %d - Err: %s", numHubWorkers, err)
	}

	for range numSubscribers {
		sub := NewSubscriber[Message]("test", maxSubBuffer)
		err = hub.Subscribe(&sub)
		if err != nil {
			fmt.Printf("Error subscribing with numSubscriber= %d - Err: %s", numSubscribers, err)
		}
	}

	for j := range numPublishers {
		wg.Add(1)
		go publisher(&wg, strconv.Itoa(j), hub, numMessagesPerPublisher, "test", "This is a testing payload")
	}

	// Let hub get fed
	for _, sub := range hub.GetSubscribers() {
		wg.Add(1)
		go consumer(&wg, sub)
	}
	wg.Wait()
	elapsed := time.Since(start)
	cancel()
	hub.Close()
	fmt.Println("Elapsed time for stress test:", elapsed)
	fmt.Println("Total Messages published and read: ", (numPublishers * numSubscribers * numMessagesPerPublisher))
	fmt.Println("Message Throughput: (msg/sec): ", float32((numPublishers*numSubscribers*numMessagesPerPublisher*1000)/int(elapsed.Milliseconds())))
}

// publisher sends a fixed number of messages into the hub using the provided identity.
func publisher(wg *sync.WaitGroup, name string, h *Hub[Message], numMessage int, topic string, payload string) {
	for i := range numMessage {
		msg, err := NewMessage(Event, name, topic, payload+strconv.Itoa(i))
		if err != nil {
			fmt.Printf("Error creating message from worker %s on message %d Err: %s", name, i, err)
			continue
		}
		err = h.Publish(*msg)
		if err != nil {
			fmt.Printf("Error publishing from worker %s on message %d Err: %s", name, i, err)
		}
	}
	wg.Done()
}

// consumer drains a subscriber's channel until it has received the expected number of messages.
func consumer(wg *sync.WaitGroup, s *Subscriber[Message]) {

	for k := range numMessagesPerPublisher * numPublishers {
		v, err := s.Channel.TryReadChannel(250 * time.Millisecond)
		if err != nil || v == nil {
			fmt.Printf("%d - Error trying to read from subscriber= %v, Msg: %v - Err: %s\n", k, s, v, err)
		}
	}
	fmt.Printf("%s - Done Reading from channel...\n", s.GetId())
	wg.Done()
}
