package mq

import (
	"context"

	"github.com/ieee0824/message-queue/message"
)

type MessageQueue[T any] interface {
	Send(msg message.Message[T]) error
	SendWithContext(ctx context.Context, msg message.Message[T]) error
	Sends(msgs []message.Message[T]) error
	SendsWithContext(ctx context.Context, msgs []message.Message[T]) error
	Receive() (*message.Message[T], error)
	ReceiveWithContext(ctx context.Context) (*message.Message[T], error)
	Receives() ([]message.Message[T], error)
	ReceivesWithContext(ctx context.Context) ([]message.Message[T], error)
	Delete(msg message.Message[T]) error
	DeleteWithContext(ctx context.Context, msg message.Message[T]) error
}
