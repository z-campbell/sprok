package main

import (
	"context"
	"fmt"
	"testing"
	"time"
)

//func TestEventHub(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	hub := NewEventHub[Message](ctx)
//	go hub.Run(1, ctx)
//	sub1 := NewSubscriber[Message]("1", 1)
//	sub2 := NewSubscriber[Message]("2", 1)
//	var wg sync.WaitGroup
//	wg.Add(1)
//	go func() {
//		v := <-sub2.Ch
//		fmt.Printf("Message received: %#v", v)
//		wg.Done()
//	}()
//	_ = hub.Subscribe(&sub1)
//	_ = hub.Subscribe(&sub2)
//	p := "exchange.MarketTick{}"
//	e := NewMessage(Unknown, "1", "2", &p)
//	err := hub.Publish(*e)
//
//	if err != nil {
//		t.Errorf("Subscribe error: %s", err)
//	}
//
//	if v, _ := hub.Topics[sub2.Topic]; len(v[0].Ch) > 1 {
//		t.Errorf("Subscriber %s should not have more than one channel", sub2.SubscriberId)
//	}
//	go func() {
//		v := <-sub2.Ch
//		fmt.Printf("Message received: %#v", v)
//		wg.Done()
//	}()
//}

func TestSubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hub := NewEventHub[Message](ctx)
	sub1 := NewSubscriber[Message]("1", 1)
	sub2 := NewSubscriber[Message]("2", 1)

	err := hub.Subscribe(&sub1)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}

	err = hub.Subscribe(&sub2)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}

	subs := hub.GetSubscribers()

	if len(subs) != 2 {
		t.Errorf("Subscribe should have two subscribers")
	}
	sub3 := NewSubscriber[Message]("1", 1)
	err = hub.Subscribe(&sub3)
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}
	err = hub.Unsubscribe(sub1)

	if err != nil {
		t.Errorf("Unsubscribe error: %s", err)
	}

	subs = hub.GetSubscribers()
	if len(subs) != 2 {
		t.Errorf("Subscribe should have one subscriber after unsubscribe")
	}
	cancel()
}

func TestBufferedChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	channel := CreateBufferedChannel[Message](ctx, 1, 10)
	err := channel.Start(ctx)
	if err != nil {
		t.Errorf("Start error: %s", err)
	}

	channel.Add(*NewMessage(Ping, "test", "test", "data"))
	channel.mu.Lock()
	if len(channel.Ch) != 1 {
		t.Errorf("Channel should have one subscriber after add")
	}

	channel.mu.Unlock()

	for i := range 15 {
		channel.Add(*NewMessage(Ping, "test", "test", fmt.Sprintf("data%d", i)))
	}

	channel.mu.Lock()

	if len(channel.Ch) != DefaultChannelBufferSize && len(*channel.Buffer) != 5 {
		t.Errorf("Channel should have one 10 and buffer should have 5")
	}

	channel.mu.Unlock()
	val, err := channel.TryReadChannel(time.Millisecond * 100)
	if err != nil {
		t.Errorf("TryReadChannel error: %s, %v", err, val)
	}
	if val.Type != Ping {
		t.Errorf("Channel should have Ping")
	}
	time.Sleep(DefaultDelayInterval * 5)
	channel.mu.Lock()
	if len(channel.Ch) != DefaultChannelBufferSize && len(*channel.Buffer) != 4 {
		t.Errorf("channel should have 10 and buffer should have 4")
	}
	channel.mu.Unlock()

	cancel()
}

func TestReadTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	channel := CreateBufferedChannel[Message](ctx, 1, 10)
	err := channel.Start(ctx)
	if err != nil {
		t.Errorf("Start error: %s", err)
	}
	_, err = channel.TryReadChannel(time.Millisecond * 0)
	if err == nil {
		t.Errorf("TryReadChannel should return error when timeout is 0 and no data is available")
	}
	cancel()
}

func TestClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	hub := NewEventHub[Message](ctx)
	client := NewClient[Message](hub, ctx)
	defer client.Close()

	if len(client.GetSubscriptions()) != 0 {
		t.Errorf("Client should have 0 subscriptions")
	}

	err := client.Subscribe("124")
	if err != nil {
		t.Errorf("Subscribe error: %s", err)
	}

	if len(client.GetSubscriptions()) == 0 {
		t.Errorf("Client should have one subscription")
	}

	cancel()
}

//func TestRouting(t *testing.T) {
//	ctx, cancel := context.WithCancel(context.Background())
//	hub := NewEventHub[Message](ctx)
//
//	cancel()
//}
