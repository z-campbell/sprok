package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"github.com/google/uuid"
	"time"
)

type MessageType int

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

type MessageFilter func(Message) bool

type MessageBase interface {
	GetType() MessageType
	GetId() uuid.UUID
	GetTimestamp() time.Time
	GetSource() string
	GetDestination() string
	GetData() []byte
}

type Message struct {
	Type        MessageType
	Id          uuid.UUID
	Timestamp   time.Time
	Source      string
	Destination string
	Data        []byte
}

func toJson[T any](t T) ([]byte, error) {
	// TODO: Can optimize this to reuse a buffer like below using sync.Pool
	data, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func toGob[T any](t T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(t); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func NewMessage(t MessageType, source string, dest string, data interface{}) *Message {
	b, err := toJson(data)
	if err != nil {
		return nil
	}
	return &Message{
		Id:          uuid.New(),
		Type:        t,
		Timestamp:   time.Now(),
		Source:      source,
		Destination: dest,
		Data:        b,
	}
}

type ErrorMessage struct {
	Message
	Error error
}

func NewErrorMessage(err error, source string) *Message {
	return NewMessage(ERROR, source, "HUB", err)
}

func (e Message) GetType() MessageType    { return e.Type }
func (e Message) GetId() uuid.UUID        { return e.Id }
func (e Message) GetTimestamp() time.Time { return e.Timestamp }
func (e Message) GetSource() string       { return e.Source }
func (e Message) GetDestination() string  { return e.Destination }
func (e Message) GetData() []byte         { return e.Data }
