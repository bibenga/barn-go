package scheduler

import (
	"database/sql"
	"log/slog"
	"time"
)

const DefaultTableName = "barn_schedule"
const DefaultIdField = "id"
const DefaultNameField = "name"
const DefaultIsActiveField = "is_active"
const DefaultCronField = "cron"
const DefaultNextRunAtField = "next_run_at"
const DefaultLastRunAtField = "last_run_at"
const DefaultFuncField = "func"
const DefaultArgsField = "args"

type ScheduleQueryConfig struct {
	TableName      string
	IdField        string
	NameField      string
	IsActiveField  string
	CronField      string
	NextRunAtField string
	LastRunAtField string
	FuncField      string
	ArgsField      string
}

type Schedule struct {
	Id        int
	Name      string
	IsActive  bool
	Cron      *string
	NextRunAt *time.Time
	LastRunAt *time.Time
	Func      string
	Args      any
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

type SchedulerRepository interface {
	FindAllActive(tx *sql.Tx) ([]*Schedule, error)
	FindOne(tx *sql.Tx, pk int) (*Schedule, error)
	Save(tx *sql.Tx, s *Schedule) error
	Delete(tx *sql.Tx, pk int) error
}

type SimpleSchedulerRepository interface {
	FindAllActiveAndUnprocessed(tx *sql.Tx, moment time.Time) ([]*Schedule, error)
	FindOne(tx *sql.Tx, pk int) (*Schedule, error)
	Save(tx *sql.Tx, s *Schedule) error
	Delete(tx *sql.Tx, pk int) error
}
