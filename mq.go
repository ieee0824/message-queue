package mq

import "github.com/ieee0824/message-queue/message"

type MessageQueue[T any] interface {
	Send(msg message.Message[T]) error
	Sends(msgs []message.Message[T]) error
	Receive() (*message.Message[T], error)
	Receives() ([]message.Message[T], error)
}
