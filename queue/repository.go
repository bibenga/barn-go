package queue

import (
	"database/sql"
	"log/slog"
	"time"
)

const DefaultTableName = "barn_message"
const DefaultIdField = "id"
const DefaultQueueField = "queue"
const DefaultCreatedField = "created_ts"
const DefaultPayloadField = "payload"
const DefaultIsProcessedField = "is_processed_flg"
const DefaultProcessedField = "processed_ts"
const DefaultIsSuccessField = "is_success_flg"
const DefaultErrorField = "error"

type MessageQueryConfig struct {
	TableName        string
	IdField          string
	QueueField       string
	CreatedTsField   string
	PayloadField     string
	IsProcessedField string
	ProcessedTsField string
	IsSuccessField   string
	ErrorField       string
}

func (c *MessageQueryConfig) init() {
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.QueueField == "" {
		c.QueueField = DefaultQueueField
	}
	if c.CreatedTsField == "" {
		c.CreatedTsField = DefaultCreatedField
	}
	if c.PayloadField == "" {
		c.PayloadField = DefaultPayloadField
	}
	if c.IsProcessedField == "" {
		c.IsProcessedField = DefaultIsProcessedField
	}
	if c.ProcessedTsField == "" {
		c.ProcessedTsField = DefaultProcessedField
	}
	if c.IsSuccessField == "" {
		c.IsSuccessField = DefaultIsSuccessField
	}
	if c.ErrorField == "" {
		c.ErrorField = DefaultErrorField
	}
}

type Message struct {
	Id          int
	Queue       *string
	Created     time.Time
	Payload     string
	IsProcessed bool
	Processed   *time.Time
	IsSuccess   *bool
	Error       *string
}

func (e Message) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	if e.Queue == nil {
		args = append(args, slog.Any("Queue", nil))
	} else {
		args = append(args, slog.String("Queue", *e.Queue))
	}
	args = append(args, slog.Time("CreatedTs", e.Created))
	args = append(args, slog.String("Payload", e.Payload))
	args = append(args, slog.Bool("IsProcessed", e.IsProcessed))
	if e.Processed == nil {
		args = append(args, slog.Any("ProcessedTs", nil))
	} else {
		args = append(args, slog.Time("ProcessedTs", *e.Processed))
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
