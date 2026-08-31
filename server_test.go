package sprok

import (
	"context"
	"fmt"
	"testing"
	"time"
)

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
	channel := CreateBufferedChannel[Message](1, 10)
	err := channel.Start(ctx)
	if err != nil {
		t.Errorf("Start error: %s", err)
	}

	channel.Add(*mustNewMessage(t, Ping, "test", "test", "data"))
	channel.mu.Lock()
	if len(channel.Ch) != 1 {
		t.Errorf("Channel should have one subscriber after add")
	}

	channel.mu.Unlock()

	for i := range 15 {
		channel.Add(*mustNewMessage(t, Ping, "test", "test", fmt.Sprintf("data%d", i)))
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
	channel := CreateBufferedChannel[Message](1, 10)
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
	client := NewClient[Message](hub)
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

func TestRouteMessageWildcardDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	sub1 := NewSubscriber[Message]("topic1", 1)
	sub2 := NewSubscriber[Message]("topic2", 1)

	if err := hub.Subscribe(&sub1); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}
	if err := hub.Subscribe(&sub2); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}

	msg := mustNewMessage(t, Event, "test", "*", "broadcast")
	if err := hub.RouteMessage(*msg); err != nil {
		t.Fatalf("RouteMessage error: %s", err)
	}

	if _, err := sub1.Channel.TryReadChannel(100 * time.Millisecond); err != nil {
		t.Errorf("subscriber on topic1 should have received wildcard-destined message: %s", err)
	}
	if _, err := sub2.Channel.TryReadChannel(100 * time.Millisecond); err != nil {
		t.Errorf("subscriber on topic2 should have received wildcard-destined message: %s", err)
	}
}

func TestUnsubscribeRemovesEmptyTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	sub := NewSubscriber[Message]("onlytopic", 1)

	if err := hub.Subscribe(&sub); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}

	if err := hub.Unsubscribe(sub); err != nil {
		t.Fatalf("Unsubscribe error: %s", err)
	}

	for _, topic := range hub.GetTopics() {
		if topic == "onlytopic" {
			t.Errorf("topic %q should have been removed after its last subscriber unsubscribed", topic)
		}
	}
}

func TestBufferedChannelStopCancelsWorkers(t *testing.T) {
	ctx := context.Background()
	channel := CreateBufferedChannel[Message](1, 10)
	if err := channel.Start(ctx); err != nil {
		t.Fatalf("Start error: %s", err)
	}

	if err := channel.Stop(); err != nil {
		t.Fatalf("Stop error: %s", err)
	}

	select {
	case <-channel.ctx.Done():
		// expected: Stop() should cancel the channel's context.
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Stop() should cancel the channel's context so workers exit")
	}
}

func TestHubEndToEndDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	if err := hub.Start(1); err != nil {
		t.Fatalf("Start error: %s", err)
	}

	sub := NewSubscriber[Message]("news", 1)
	if err := hub.Subscribe(&sub); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}

	msg := mustNewMessage(t, Event, "publisher", "news", "hello")
	if err := hub.Publish(*msg); err != nil {
		t.Fatalf("Publish error: %s", err)
	}

	val, err := sub.Channel.TryReadChannel(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("subscriber should have received the published message: %s", err)
	}
	if val.Destination != "news" {
		t.Errorf("expected message destined for 'news', got %q", val.Destination)
	}
}

func TestHubRouteMessageInvalidDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	msg := mustNewMessage(t, Event, "publisher", "nonexistent-topic", "hello")
	if err := hub.RouteMessage(*msg); err == nil {
		t.Errorf("expected an error routing a message to a topic with no subscribers")
	}
}

func TestHubClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	if err := hub.Start(1); err != nil {
		t.Fatalf("Start error: %s", err)
	}

	sub := NewSubscriber[Message]("topic", 1)
	if err := hub.Subscribe(&sub); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}

	done := make(chan struct{})
	go func() {
		hub.Close()
		close(done)
	}()

	select {
	case <-done:
		// expected: Close() returns once workers and subscribers have shut down.
	case <-time.After(time.Second):
		t.Fatal("Hub.Close() should return once workers and subscribers have shut down")
	}
}

func TestRemoveSubscribers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	sub1 := NewSubscriber[Message]("topic", 1)
	sub2 := NewSubscriber[Message]("topic", 1)

	if err := hub.Subscribe(&sub1); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}
	if err := hub.Subscribe(&sub2); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}

	hub.RemoveSubscribers([]*Subscriber[Message]{&sub1, &sub2})

	if len(hub.GetSubscribers()) != 0 {
		t.Errorf("expected 0 subscribers after RemoveSubscribers, got %d", len(hub.GetSubscribers()))
	}
	for _, topic := range hub.GetTopics() {
		if topic == "topic" {
			t.Errorf("topic %q should have been removed once its last subscriber was removed", topic)
		}
	}
}

func TestHubErrorChannelReceivesRoutingErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	if err := hub.Start(1); err != nil {
		t.Fatalf("Start error: %s", err)
	}

	msg := mustNewMessage(t, Event, "publisher", "nonexistent-topic", "hello")
	if err := hub.Publish(*msg); err != nil {
		t.Fatalf("Publish error: %s", err)
	}

	errMsg, err := hub.Errors.TryReadChannel(500 * time.Millisecond)
	if err != nil {
		t.Fatalf("expected a routing error to be delivered on the error channel: %s", err)
	}
	if errMsg.Type != ERROR {
		t.Errorf("expected error channel message of type ERROR, got %v", errMsg.Type)
	}
}

func TestClientUnsubscribeAndClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hub := NewEventHub[Message](ctx)
	client := NewClient[Message](hub)

	if err := client.Subscribe("topicA"); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}
	if err := client.Subscribe("topicB"); err != nil {
		t.Fatalf("Subscribe error: %s", err)
	}
	if len(client.GetSubscriptions()) != 2 {
		t.Fatalf("expected 2 subscriptions, got %d", len(client.GetSubscriptions()))
	}

	if err := client.Unsubscribe("topicA"); err != nil {
		t.Fatalf("Unsubscribe error: %s", err)
	}
	if len(client.GetSubscriptions()) != 1 {
		t.Errorf("expected 1 subscription after unsubscribe, got %d", len(client.GetSubscriptions()))
	}

	if err := client.Unsubscribe("missing-topic"); err == nil {
		t.Errorf("expected an error unsubscribing from a topic the client is not subscribed to")
	}

	client.Close()
	if len(hub.GetSubscribers()) != 0 {
		t.Errorf("expected hub to have 0 subscribers after client Close(), got %d", len(hub.GetSubscribers()))
	}
}

// mustNewMessage creates a message and fails the test immediately if creation errors.
func mustNewMessage(t *testing.T, mt MessageType, source, dest string, data interface{}) *Message {
	t.Helper()
	msg, err := NewMessage(mt, source, dest, data)
	if err != nil {
		t.Fatalf("NewMessage error: %s", err)
	}
	return msg
}

func TestMessageUnmarshal(t *testing.T) {
	msg := mustNewMessage(t, Event, "test", "topic", "payload-string")

	var got string
	if err := msg.Unmarshal(&got); err != nil {
		t.Fatalf("Unmarshal error: %s", err)
	}
	if got != "payload-string" {
		t.Errorf("expected %q, got %q", "payload-string", got)
	}
}
