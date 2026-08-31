package sprok

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"time"
)

// MessageType identifies the semantic kind of a Message (event, request, response, etc.).
type MessageType int

// MessageFilter reports whether a Message should be included when applied as a predicate.
type MessageFilter func(Message) bool

const (
	Unknown MessageType = iota
	Event
	Request
	Response
	Tick
	Ping
	Pong
	ERROR
)

// MessageBase is the interface all message types must satisfy to be routed through a Hub.
type MessageBase interface {
	GetType() MessageType
	GetId() uuid.UUID
	GetTimestamp() time.Time
	GetSource() string
	GetDestination() string
	GetData() []byte
}

// Message is the default, JSON-encoded implementation of MessageBase used throughout the hub.
type Message struct {
	Type        MessageType
	Id          uuid.UUID
	Timestamp   time.Time
	Source      string
	Destination string
	Data        []byte
}

// toJson marshals a value into JSON bytes so it can be carried inside a Message.
func toJson[T any](t T) ([]byte, error) {
	// TODO: Can optimize this to reuse a buffer like below using sync.Pool
	data, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// toGob encodes a value using the gob format.
func toGob[T any](t T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(t); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// NewMessage creates a message with a generated ID, timestamp, source, destination, and serialized payload.
// It returns an error if data cannot be marshaled to JSON.
func NewMessage(t MessageType, source string, dest string, data interface{}) (*Message, error) {
	b, err := toJson(data)
	if err != nil {
		return nil, fmt.Errorf("unable to create message: %w", err)
	}
	return &Message{
		Id:          uuid.New(),
		Type:        t,
		Timestamp:   time.Now(),
		Source:      source,
		Destination: dest,
		Data:        b,
	}, nil
}

type ErrorMessage struct {
	Message
	Error error
}

// NewErrorMessage wraps an error in a message so it can be routed through the hub's error channel.
// Marshaling failures fall back to a message carrying the error's string representation.
func NewErrorMessage(err error, source string) *Message {
	msg, marshalErr := NewMessage(ERROR, source, "HUB", err.Error())
	if marshalErr != nil {
		// This should be unreachable since err.Error() always marshals to a JSON string,
		// but fall back to a minimal message rather than returning nil.
		return &Message{Id: uuid.New(), Type: ERROR, Timestamp: time.Now(), Source: source, Destination: "HUB"}
	}
	return msg
}

// GetType returns the message's semantic type.
func (e Message) GetType() MessageType { return e.Type }

// GetId returns the message's unique identifier.
func (e Message) GetId() uuid.UUID { return e.Id }

// GetTimestamp returns when the message was created.
func (e Message) GetTimestamp() time.Time { return e.Timestamp }

// GetSource returns the message's origin.
func (e Message) GetSource() string { return e.Source }

// GetDestination returns the topic or subscriber target for the message.
func (e Message) GetDestination() string { return e.Destination }

// GetData returns the serialized payload carried by the message.
func (e Message) GetData() []byte { return e.Data }

// Unmarshal decodes the message's JSON-encoded payload into v.
func (e Message) Unmarshal(v interface{}) error {
	return json.Unmarshal(e.Data, v)
}
