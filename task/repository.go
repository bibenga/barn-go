package task

import (
	"database/sql"
	"log/slog"
	"time"
)

type Status uint8

const ()

const DefaultTableName = "barn_task"
const DefaultIdField = "id"
const DefaultRunAtField = "run_at"
const DefaultFuncField = "func"
const DefaultArgsField = "args"
const DefaultIsProcessedField = "is_processed"
const DefaultStartedAtField = "started_at"
const DefaultFinishedAtField = "finished_at"
const DefaultIsSuccessField = "is_success_flg"
const DefaultResultField = "result"
const DefaultErrorField = "error"

type TaskQueryConfig struct {
	TableName        string
	IdField          string
	RunAtField       string
	FuncField        string
	ArgsField        string
	IsProcessedField string
	StartedAtField   string
	FinishedAtField  string
	IsSuccessField   string
	ResultField      string
	ErrorField       string
}

type Task struct {
	Id          int
	RunAt       time.Time
	Func        string
	Args        any
	IsProcessed bool
	StartedAt   *time.Time
	FinishedAt  *time.Time
	IsSuccess   *bool
	Result      any
	Error       *string
}

func (e Task) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.Time("RunAt", e.RunAt))
	args = append(args, slog.String("Func", e.Func))
	args = append(args, slog.Any("Args", e.Args))
	args = append(args, slog.Bool("IsProcessed", e.IsProcessed))
	if e.StartedAt == nil {
		args = append(args, slog.Any("StartedAt", nil))
	} else {
		args = append(args, slog.Time("StartedAt", *e.StartedAt))
	}
	if e.FinishedAt == nil {
		args = append(args, slog.Any("FinishedAt", nil))
	} else {
		args = append(args, slog.Time("FinishedAt", *e.FinishedAt))
	}
	if e.IsSuccess == nil {
		args = append(args, slog.Any("IsSuccess", nil))
	} else {
		args = append(args, slog.Bool("IsSuccess", *e.IsSuccess))
	}
	args = append(args, slog.Any("Result", e.Result))
	if e.Error == nil {
		args = append(args, slog.Any("Error", nil))
	} else {
		args = append(args, slog.String("Error", *e.Error))
	}
	return slog.GroupValue(args...)
}

type TaskRepository interface {
	FindNext(tx *sql.Tx) (*Task, error)
	Create(tx *sql.Tx, task *Task) error
	Save(tx *sql.Tx, task *Task) error
	DeleteOld(tx *sql.Tx, t time.Time) (int, error)
}
