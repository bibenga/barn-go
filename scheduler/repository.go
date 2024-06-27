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
const DefaultMessageField = "message"

type ScheduleQueryConfig struct {
	TableName      string
	IdField        string
	NameField      string
	IsActiveField  string
	CronField      string
	NextRunAtField string
	LastRunAtField string
	MessageField   string
}

func (c *ScheduleQueryConfig) init() {
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.NameField == "" {
		c.NameField = DefaultNameField
	}
	if c.IsActiveField == "" {
		c.IsActiveField = DefaultIsActiveField
	}
	if c.CronField == "" {
		c.CronField = DefaultCronField
	}
	if c.NextRunAtField == "" {
		c.NextRunAtField = DefaultNextRunAtField
	}
	if c.LastRunAtField == "" {
		c.LastRunAtField = DefaultLastRunAtField
	}
	if c.MessageField == "" {
		c.MessageField = DefaultMessageField
	}
}

type Schedule struct {
	Id        int
	Name      string
	IsActive  bool
	Cron      *string
	NextRunAt *time.Time
	LastRunAt *time.Time
	Message   *string
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
	if e.Message == nil {
		args = append(args, slog.Any("Message", nil))
	} else {
		args = append(args, slog.String("Message", *e.Message))
	}
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
