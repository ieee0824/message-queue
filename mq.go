package mq

import (
	"context"

	"github.com/ieee0824/message-queue/message"
)

type MessageQueue[T any] interface {
	Send(ctx context.Context, msgs message.Message[T]) error
	Receives(ctx context.Context) ([]message.Message[T], error)
	Delete(ctx context.Context, msg message.Message[T]) error
}
