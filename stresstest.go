package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

const (
	numPublishers           = 10
	numSubscribers          = 10
	numMessagesPerPublisher = 1_000
	numHubWorkers           = 10
	maxSubBuffer            = 10
)

func StressTest() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	hub := NewEventHub[Message](ctx)
	err := hub.Start(numHubWorkers, ctx)
	start := time.Now()
	if err != nil {
		fmt.Printf("Error starting hub with numWorkers= %d - Err: %s", numHubWorkers, err)
	}

	for i := 0; i < numSubscribers; i++ {
		sub := NewSubscriber[Message]("test", maxSubBuffer)
		err = hub.Subscribe(&sub)
		if err != nil {
			fmt.Printf("Error subscribing with numSubscriber= %d - Err: %s", numSubscribers, err)
		}
	}
	for j := range numPublishers {
		go publisher(&wg, strconv.Itoa(j), hub, numMessagesPerPublisher, "test", "This is a testing payload")
		wg.Add(1)
	}

	for _, sub := range hub.GetSubscribers() {
		go consumer(&wg, sub)
		wg.Add(1)
	}
	//time.Sleep(10 * time.Second)
	wg.Wait()
	cancel()
	elapsed := time.Since(start)
	fmt.Println("Elapsed time for stress test:", elapsed)
	fmt.Println("Total Messages published and read: ", (numPublishers * numSubscribers * numMessagesPerPublisher))
}

func publisher(wg *sync.WaitGroup, name string, h *Hub[Message], numMessage int, topic string, payload string) {
	for i := range numMessage {
		err := h.Publish(*NewMessage(Event, name, topic, payload+strconv.Itoa(i)))
		if err != nil {
			fmt.Printf("Error publishing from worker %s on message %d Err: %s", name, i, err)
		}
	}
	wg.Done()
}

func consumer(wg *sync.WaitGroup, s *Subscriber[Message]) {

	for k := range numMessagesPerPublisher * numPublishers {
		v, err := s.Channel.TryReadChannel(5 * time.Millisecond)
		if err != nil {
			fmt.Printf("%d - Error trying to read from subscriber= %v, Msg: %v - Err: %s\n", k, s, v, err)
		}
	}
	fmt.Printf("%s - Done Reading from channel...\n", s.GetId())
	wg.Done()
}
