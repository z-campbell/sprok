# sprok

`sprok` is a small, generic, type-safe pub/sub message hub for Go. It
routes messages between topic-based subscribers using a background worker pool
and a buffered channel primitive absorbs bursts of traffic
without blocking publishers.

## Features

- **Generic hub and subscribers** — parameterized over any type implementing
  `MessageBase`, so you can route your own message types, not just the built-in
  `Message`.
- **Topic-based routing** — subscribers register for a topic; publishing a
  message to that topic fans it out to every subscriber on it.
- **Wildcard broadcast** — publishing with destination `"*"` delivers to every
  subscriber regardless of topic; subscribing to topic `"*"` receives every
  message regardless of destination.
- **Buffered, non-blocking delivery** — `BufferedChannel` overflows into an
  internal buffer (drained by background workers) instead of blocking the
  caller when a channel is full.
- **Client convenience wrapper** — `Client` tracks a set of subscriptions on
  behalf of a single consumer and cleans them all up on `Close()`.
- **Error channel** — routing errors are delivered as messages on
  `Hub.Errors` instead of being silently dropped.

## Installation

```sh
go get github.com/z-campbell/sprok
```

## Quick start

```go
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

	hub := sprok.NewEventHub[sprok.Message](ctx)
	if err := hub.Start(sprok.DefaultHubWorkerCount); err != nil {
		panic(err)
	}
	defer hub.Close()

	sub := sprok.NewSubscriber[sprok.Message]("greetings", 10)
	if err := hub.Subscribe(&sub); err != nil {
		panic(err)
	}

	msg, err := sprok.NewMessage(sprok.Event, "example", "greetings", "hello, sprok!")
	if err != nil {
		panic(err)
	}
	if err := hub.Publish(*msg); err != nil {
		panic(err)
	}

	received, err := sub.Channel.TryReadChannel(time.Second)
	if err != nil {
		panic(err)
	}

	var payload string
	_ = received.Unmarshal(&payload)
	fmt.Println(payload) // "hello, sprok!"
}
```

A runnable version of this example lives in
[cmd/example](/Users/zach/source/sprok/cmd/example). Run it with:

```sh
go run ./cmd/example
```

For a higher-level API that tracks subscriptions for you, see `Client` in
[client.go](/Users/zach/source/sprok/client.go):

```go
client := sprok.NewClient[sprok.Message](hub)
defer client.Close()

_ = client.Subscribe("greetings")
_ = client.Unsubscribe("greetings")
```

## Architecture

| Type              | File                                                      | Responsibility |
|-------------------|-----------------------------------------------------------|----------------|
| `Hub`             | [server.go](/Users/zach/source/sprok/server.go)           | Owns subscribers and topics; routes published messages to the right subscriber channel(s) via a worker pool. |
| `Subscriber`      | [subscriber.go](/Users/zach/source/sprok/subscriber.go)   | A single topic registration backed by its own `BufferedChannel`. |
| `Client`          | [client.go](/Users/zach/source/sprok/client.go)           | Tracks a set of subscriptions for one consumer and tears them all down together. |
| `BufferedChannel` | [channels.go](/Users/zach/source/sprok/channels.go)       | A channel with an overflow buffer and background drain workers, so `Add` never blocks the caller. |
| `Message`         | [messages.go](/Users/zach/source/sprok/messages.go)       | The default `MessageBase` implementation; JSON-encodes an arbitrary payload. |

Messages flow as follows:

1. A publisher calls `Hub.Publish`, which enqueues the message on the hub's
   internal `BufferedChannel`.
2. One of the hub's `runWorker` goroutines picks it up and calls
   `RouteMessage`, which delivers it to:
   - every subscriber on topic `"*"`,
   - every subscriber if the message's destination is `"*"`, or
   - every subscriber registered on the message's destination topic.
3. Each subscriber's own `BufferedChannel` receives the message, and the
   subscriber's owner reads it with `TryReadChannel`.
4. Any routing error is wrapped in an `ErrorMessage` and delivered on
   `Hub.Errors` instead of being dropped.

## Development

Run the test suite (with the race detector, as concurrency is central to this
package):

```sh
go test -race ./...
```

Build the CLI binaries:

```sh
make build
```

There's also a synthetic load generator you can run to get a rough throughput
number for your machine:

```sh
go run ./cmd/stresstest
```

## License

MIT — see [LICENSE](/Users/zach/source/sprok/LICENSE).
