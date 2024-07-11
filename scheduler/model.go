package scheduler

import (
	"log/slog"
	"time"
)

type Schedule struct {
	Id        int        `barn:""`
	Name      string     `barn:""`
	IsActive  bool       `barn:""`
	Cron      *string    `barn:""`
	NextRunAt *time.Time `barn:""`
	LastRunAt *time.Time `barn:""`
	Func      string     `barn:""`
	Args      any        `barn:""`
}

func (e Schedule) TableName() string {
	return "barn_schedule"
}

func (e Schedule) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.String("Name", e.Name))
	args = append(args, slog.Bool("IsActive", e.IsActive))
	if e.Cron == nil {
		args = append(args, slog.Any("Cron", nil))
	} else {
		args = append(args, slog.String("Cron", *e.Cron))
	}
	if e.NextRunAt == nil {
		args = append(args, slog.Any("NextRunAt", nil))
	} else {
		args = append(args, slog.Time("NextRunAt", *e.NextRunAt))
	}
	if e.LastRunAt == nil {
		args = append(args, slog.Any("LastRunAt", nil))
	} else {
		args = append(args, slog.Time("LastRunAt", *e.LastRunAt))
	}
	args = append(args, slog.String("Func", e.Func))
	args = append(args, slog.Any("Args", e.Args))
	return slog.GroupValue(args...)
}
