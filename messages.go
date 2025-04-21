package main

import (
	"github.com/google/uuid"
	"time"
)

type MessageType int

const (
	Unknown MessageType = iota
	Request
	Tick
	Response
	Ping
	Pong
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

type ErrorMessage struct {
	Message
	Error error
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

func (e Message) GetType() MessageType    { return e.Type }
func (e Message) GetId() uuid.UUID        { return e.Id }
func (e Message) GetTimestamp() time.Time { return e.Timestamp }
func (e Message) GetSource() string       { return e.Source }
func (e Message) GetDestination() string  { return e.Destination }
func (e Message) GetData() interface{}    { return e.Data }
