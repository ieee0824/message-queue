package rdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/ieee0824/message-queue/message"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type QueueMessage struct {
	gorm.Model
	Body        string `gorm:"index:idx_queue_message_body,unique"`
	QueueName   string `gorm:"index:idx_queue_message_queue_name"`
	CalledCount int
	DeleteTag   string `gorm:"index:idx_queue_message_delete_tag,unique"`
}

type DBMode int

const (
	ModeSQLite DBMode = iota
)

func New[T any](dbName string, mode DBMode, queueName string) *RDBDriver[T] {
	if dbName == "" {
		log.Fatal().Msg("db name is empty")
	}
	if queueName == "" {
		log.Fatal().Msg("queue name is empty")
	}
	var db *gorm.DB
	switch mode {
	default:
		sqLite, err := gorm.Open(sqlite.Open(dbName), &gorm.Config{})
		if err != nil {
			log.Fatal().Err(err).Msg("failed to open db")
		}
		db = sqLite
	}

	if err := db.AutoMigrate(&QueueMessage{}); err != nil {
		log.Fatal().Err(err).Msg("failed to migrate")
	}

	return &RDBDriver[T]{
		db:                db,
		queueName:         queueName,
		maxProcessingTime: 10 * time.Second,
		maxCalledCount:    10,
		maxReceiveCount:   10,
	}
}

type RDBDriver[T any] struct {
	queueName         string
	db                *gorm.DB
	maxProcessingTime time.Duration
	maxCalledCount    int
	maxReceiveCount   int
}

func (impl *RDBDriver[T]) convertBodyStr(msg message.Message[T]) (string, error) {
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(msg)
	if err != nil {
		return "", fmt.Errorf("failed to encode message: %w", err)
	}

	return buf.String(), nil
}

func (impl *RDBDriver[T]) Send(octx context.Context, msg T) error {
	body, err := impl.convertBodyStr(message.Message[T]{
		Body: msg,
	})
	if err != nil {
		return fmt.Errorf("failed to convert body: %w", err)
	}

	queueMessage := QueueMessage{
		Body:        body,
		QueueName:   impl.queueName,
		CalledCount: 0,
		DeleteTag:   uuid.NewString(),
	}

	ctx, cancel := context.WithTimeout(octx, impl.maxProcessingTime)
	defer cancel()

	if err := impl.db.WithContext(ctx).Create(&queueMessage).Error; err != nil {
		return fmt.Errorf("failed to create queue message: %w", err)
	}

	return nil
}

func (impl *RDBDriver[T]) Receives(octx context.Context) ([]message.Message[T], error) {

	ctx, cancel := context.WithTimeout(octx, impl.maxProcessingTime)
	defer cancel()

	var ret []message.Message[T]
	err := impl.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var messages []QueueMessage
		if err := tx.Where("queue_name = ?", impl.queueName).Where("called_count < ?", impl.maxCalledCount).Find(&messages).Error; err != nil {
			return fmt.Errorf("failed to find queue messages: %w", err)
		}
		sort.Slice(messages, func(i, j int) bool {
			return messages[i].UpdatedAt.Before(messages[j].UpdatedAt)
		})

		ret = lo.FilterMap(lo.Filter(messages, func(msg QueueMessage, _ int) bool {
			// 現在時刻が更新時刻よりmaxProcessingTime以上過義弟ない場合false
			isBefore := msg.UpdatedAt.Add(impl.maxProcessingTime).Before(time.Now())
			return isBefore
		}), func(qm QueueMessage, i int) (message.Message[T], bool) {
			if i >= impl.maxReceiveCount {
				return message.Message[T]{}, false
			}
			msg := message.Message[T]{}

			if err := json.NewDecoder(bytes.NewBufferString(qm.Body)).Decode(&msg); err != nil {
				log.Error().Err(err).Msg("failed to decode message")
				return msg, true
			}

			msg.SetDeleteTag(qm.DeleteTag)

			return msg, true
		})
		if len(ret) == 0 {
			return errors.New("no messages")
		}
		deleteTags := lo.Map(ret, func(msg message.Message[T], _ int) string {
			return msg.DeleteTag()
		})
		if err := tx.Exec(
			`UPDATE queue_messages SET called_count = called_count + 1 WHERE delete_tag IN (?)`,
			deleteTags,
		).Error; err != nil {
			return fmt.Errorf("failed to update queue messages: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to transaction: %w", err)
	}
	return ret, nil
}

func (impl *RDBDriver[T]) Delete(octx context.Context, msg message.Message[T]) error {
	ctx, cancel := context.WithTimeout(octx, impl.maxProcessingTime)
	defer cancel()

	err := impl.db.WithContext(ctx).Transaction(
		func(tx *gorm.DB) error {
			target := QueueMessage{}
			if err := tx.Where("delete_tag = ?", msg.DeleteTag()).First(&target).Error; err != nil {
				return fmt.Errorf("failed to find queue message: %w", err)
			}

			// updated_at + maxProcessingTimeが現在時刻より未来の場合は削除できる
			if target.UpdatedAt.Add(impl.maxProcessingTime).Before(time.Now()) {
				err := tx.Delete(&target).Error
				if err != nil {
					return fmt.Errorf("failed to delete queue message: %w", err)
				}
			} else {
				return errors.New("time out")
			}
			return nil
		},
	)
	if err != nil {
		return fmt.Errorf("failed to transaction: %w", err)
	}
	return nil
}
