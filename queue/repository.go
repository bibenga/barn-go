package queue

import (
	"database/sql"
	"log/slog"
	"time"
)

const DefaultTableName = "barn_message"
const DefaultIdField = "id"
const DefaultCreatedAtField = "created_at"
const DefaultPayloadField = "payload"
const DefaultIsProcessedField = "is_processed"
const DefaultProcessedAtField = "processed_at"
const DefaultIsSuccessField = "is_success_flg"
const DefaultErrorField = "error"

type MessageQueryConfig struct {
	TableName        string
	IdField          string
	CreatedAtField   string
	PayloadField     string
	IsProcessedField string
	ProcessedAtField string
	IsSuccessField   string
	ErrorField       string
}

type Message struct {
	Id          int
	CreatedAt   time.Time
	Payload     string
	IsProcessed bool
	ProcessedAt *time.Time
	IsSuccess   *bool
	Error       *string
}

func (e Message) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.Time("CreatedAt", e.CreatedAt))
	args = append(args, slog.String("Payload", e.Payload))
	args = append(args, slog.Bool("IsProcessed", e.IsProcessed))
	if e.ProcessedAt == nil {
		args = append(args, slog.Any("ProcessedAt", nil))
	} else {
		args = append(args, slog.Time("ProcessedAt", *e.ProcessedAt))
	}
	if e.IsSuccess == nil {
		args = append(args, slog.Any("IsSuccess", nil))
	} else {
		args = append(args, slog.Bool("IsSuccess", *e.IsSuccess))
	}
	if e.Error == nil {
		args = append(args, slog.Any("Error", nil))
	} else {
		args = append(args, slog.String("Error", *e.Error))
	}
	return slog.GroupValue(args...)
}

type MessageRepository interface {
	FindNext(tx *sql.Tx) (*Message, error)
	Create(tx *sql.Tx, m *Message) error
	Save(tx *sql.Tx, m *Message) error
	DeleteProcessed(tx *sql.Tx, t time.Time) (int, error)
}
