package task

import (
	"log/slog"
	"time"
)

type Status string

const (
	Queued Status = "Q"
	Done   Status = "D"
	Failed Status = "F"
)

type Task struct {
	Id         int        `barn:""`
	RunAt      time.Time  `barn:""`
	Func       string     `barn:""`
	Args       any        `barn:""`
	Status     Status     `barn:""`
	StartedAt  *time.Time `barn:""`
	FinishedAt *time.Time `barn:""`
	Result     any        `barn:""`
	Error      *string    `barn:""`
}

func (e Task) TableName() string {
	return "barn_task"
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
