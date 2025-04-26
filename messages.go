package main

import (
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
	GetData() interface{}
}

type Message struct {
	Type        MessageType
	Id          uuid.UUID
	Timestamp   time.Time
	Source      string
	Destination string
	Data        interface{}
}

func NewMessage(t MessageType, source string, dest string, data interface{}) *Message {
	return &Message{
		Id:          uuid.New(),
		Type:        t,
		Timestamp:   time.Now(),
		Source:      source,
		Destination: dest,
		Data:        data,
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
func (e Message) GetData() interface{}    { return e.Data }
