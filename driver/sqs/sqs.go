package sqs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/ieee0824/message-queue/message"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type OptFunc func(*Option)

type Option struct {
	ClientTimeout *time.Duration
}

func (impl *Option) def() {
	if impl.ClientTimeout == nil {
		impl.ClientTimeout = lo.ToPtr(10 * time.Second)
	}
}

func New[T any](queueName string, optFuncs ...OptFunc) *SQSDriver[T] {
	if queueName == "" {
		log.Fatal().Msg("queueName is empty")
	}
	var opt Option

	for _, optFunc := range optFuncs {
		optFunc(&opt)
	}
	opt.def()

	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load aws config")
	}

	client := sqs.NewFromConfig(awsCfg)
	getQueueURLResult, err := client.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to get queue url")
	}

	return &SQSDriver[T]{
		client:        client,
		clientTimeout: *opt.ClientTimeout,
		queueURL:      *getQueueURLResult.QueueUrl,
	}

}

type SQSDriver[T any] struct {
	client        *sqs.Client
	clientTimeout time.Duration
	queueURL      string
}

func (impl *SQSDriver[T]) convertMsgStr(msg message.Message[T]) (string, error) {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(msg)
	if err != nil {
		return "", fmt.Errorf("failed to encode message: %w", err)
	}
	return buf.String(), nil
}

func (impl *SQSDriver[T]) parseMsgStr(msgStr string) (message.Message[T], error) {
	var msg message.Message[T]
	err := json.NewDecoder(bytes.NewBufferString(msgStr)).Decode(&msg)
	if err != nil {
		return message.Message[T]{}, fmt.Errorf("failed to decode message: %w", err)
	}
	return msg, nil
}

func (impl *SQSDriver[T]) Send(octx context.Context, msgs message.Message[T]) error {
	ctx, cancel := context.WithTimeout(octx, impl.clientTimeout)
	defer cancel()

	msgStr, err := impl.convertMsgStr(msgs)
	if err != nil {
		return fmt.Errorf("failed to convert message: %w", err)
	}

	_, err = impl.client.SendMessage(ctx, &sqs.SendMessageInput{
		MessageBody: &msgStr,
		QueueUrl:    &impl.queueURL,
	})
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

func (impl *SQSDriver[T]) Receives(octx context.Context) ([]message.Message[T], error) {
	ctx, cancel := context.WithTimeout(octx, impl.clientTimeout)
	defer cancel()

	resp, err := impl.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: &impl.queueURL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}

	var msgs []message.Message[T]
	for _, rawMsg := range resp.Messages {
		msg, err := impl.parseMsgStr(*rawMsg.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to parse message: %w", err)
		}
		msg.SetDeleteTag(*rawMsg.ReceiptHandle)

		msgs = append(msgs, msg)
	}

	return msgs, nil
}

func (impl *SQSDriver[T]) Delete(octx context.Context, msg message.Message[T]) error {
	ctx, cancel := context.WithTimeout(octx, impl.clientTimeout)
	defer cancel()

	_, err := impl.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      &impl.queueURL,
		ReceiptHandle: lo.ToPtr(msg.DeleteTag()),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}
