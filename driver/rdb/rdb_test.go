package rdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSend(t *testing.T) {
	driver := New[string]("test_send.db", ModeSQLite, "test_send")
	defer func() {
		os.RemoveAll("test_send.db")
	}()

	tests := []struct {
		name  string
		body  string
		isErr bool
	}{
		{
			name:  "success",
			body:  "test",
			isErr: false,
		},
		{
			name:  "bodyが同一の場合はエラー",
			body:  "test",
			isErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := driver.Send(
				context.Background(),
				tt.body,
			)

			if tt.isErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestReceive(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		waitTime time.Duration
		isErr    bool
	}{
		{
			name:     "success",
			body:     "test",
			waitTime: 11 * time.Second,
			isErr:    false,
		},
		{
			name:     "waitが短すぎてエラー",
			body:     "test2",
			waitTime: 1 * time.Second,
			isErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbName := "test_receive.db"
			driver := New[string](dbName, ModeSQLite, "test_receive")
			defer func() {
				os.RemoveAll(dbName)
			}()
			if err := driver.Send(context.Background(), tt.body); err != nil {
				t.Fatal(err)
			}

			time.Sleep(tt.waitTime)

			msgs, err := driver.Receives(context.Background())
			if tt.isErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)

				assert.Equal(t, tt.body, msgs[0].Body)
			}
		})
	}
}

func TestDelete(t *testing.T) {
	tests := []struct {
		name string
		body string
		wait time.Duration
	}{
		{
			name: "success",
			body: "test",
			wait: 11 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := New[string]("test_delete.db", ModeSQLite, "test_delete")
			defer func() {
				os.RemoveAll("test_delete.db")
			}()

			if err := driver.Send(context.Background(), tt.body); err != nil {
				t.Fatal(err)
			}
			time.Sleep(tt.wait)

			msgs, err := driver.Receives(context.Background())
			if err != nil {
				t.Fatal(err)
			}

			if err := driver.Delete(context.Background(), msgs[0]); err != nil {
				t.Fatal(err)
			}
		})
	}
}
