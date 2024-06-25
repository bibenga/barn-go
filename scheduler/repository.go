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
const DefaultNextTsField = "next_ts"
const DefaultLastTsField = "last_ts"
const DefaultMessageField = "message"

type ScheduleQueryConfig struct {
	TableName     string
	IdField       string
	NameField     string
	IsActiveField string
	CronField     string
	NextTsField   string
	LastTsField   string
	MessageField  string
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
	if c.NextTsField == "" {
		c.NextTsField = DefaultNextTsField
	}
	if c.LastTsField == "" {
		c.LastTsField = DefaultLastTsField
	}
	if c.MessageField == "" {
		c.MessageField = DefaultMessageField
	}
}

type ScheduleQuery struct {
	CreateTableQuery    string
	SelectQuery         string
	InsertQuery         string
	DeleteQuery         string
	DeleteAllQuery      string
	UpdateQuery         string
	UpdateIsActiveQuery string
}

type Schedule struct {
	Id       int
	Name     string
	IsActive bool
	Cron     *string
	NextTs   *time.Time
	LastTs   *time.Time
	Message  *string
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
	if e.NextTs == nil {
		args = append(args, slog.Any("NextTs", nil))
	} else {
		args = append(args, slog.Time("NextTs", *e.NextTs))
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
	FindAllActiveToProcess(tx *sql.Tx, moment time.Time) ([]*Schedule, error)
	FindOne(tx *sql.Tx, pk int) (*Schedule, error)
	Create(tx *sql.Tx, s *Schedule) error
	Save(tx *sql.Tx, s *Schedule) error
	Delete(tx *sql.Tx, pk int) error
}
