package task

import (
	"database/sql"
	"log/slog"
	"time"
)

type Status string

const (
	Queued Status = "Q"
	Done   Status = "D"
	Failed Status = "F"
)

const DefaultTableName = "barn_task"
const DefaultIdField = "id"
const DefaultRunAtField = "run_at"
const DefaultFuncField = "func"
const DefaultArgsField = "args"
const DefaultStatusField = "status"
const DefaultStartedAtField = "started_at"
const DefaultFinishedAtField = "finished_at"
const DefaultResultField = "result"
const DefaultErrorField = "error"

type TaskQueryConfig struct {
	TableName       string
	IdField         string
	RunAtField      string
	FuncField       string
	ArgsField       string
	StatusField     string
	StartedAtField  string
	FinishedAtField string
	ResultField     string
	ErrorField      string
}

type Task struct {
	Id         int
	RunAt      time.Time
	Func       string
	Args       any
	Status     Status
	StartedAt  *time.Time
	FinishedAt *time.Time
	Result     any
	Error      *string
}

func (e Task) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.Time("RunAt", e.RunAt))
	args = append(args, slog.String("Func", e.Func))
	args = append(args, slog.Any("Args", e.Args))
	args = append(args, slog.Any("Status", e.Status))
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
