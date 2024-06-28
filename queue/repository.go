package queue

import (
	"database/sql"
	"log/slog"
	"time"
)

const DefaultTableName = "barn_queue"
const DefaultIdField = "id"
const DefaultCreatedAtField = "created_at"
const DefaultPayloadField = "payload"

type QueueQueryConfig struct {
	TableName      string
	IdField        string
	CreatedAtField string
	PayloadField   string
}

type Message struct {
	Id        int
	CreatedAt time.Time
	Payload   any
}

func (e Message) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.Time("CreatedAt", e.CreatedAt))
	args = append(args, slog.Any("Payload", e.Payload))
	return slog.GroupValue(args...)
}

type QueueRepository interface {
	FindNext(tx *sql.Tx) (*Message, error)
	Create(tx *sql.Tx, message *Message) error
	Delete(tx *sql.Tx, message *Message) error
}
