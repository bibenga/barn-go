package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/task"
)

type SchedulerHandler2[S any] func(tx *sql.Tx, schedule *S) error

func dummySchedulerHandler2[S any](tx *sql.Tx, s *S) error {
	slog.Debug("DUMMY: process", "schedule", s)
	return nil
}

var _ SchedulerHandler = dummySchedulerHandler

type SchedulerConfig2[S any] struct {
	Log     *slog.Logger
	Cron    string
	Handler SchedulerHandler2[S]
}

type Scheduler2[S any] struct {
	log     *slog.Logger
	handler SchedulerHandler2[S]
	db      *sql.DB
	meta    task.TaskQueryConfig
	running atomic.Bool
	cancel  context.CancelFunc
	stoped  sync.WaitGroup
	cron    string
	nextTs  time.Time
}

func NewSimpleScheduler2[S any](db *sql.DB, config ...SchedulerConfig2[S]) *Scheduler2[S] {
	var log *slog.Logger
	var cron string
	var handler SchedulerHandler2[S]
	if len(config) == 1 {
		log = config[0].Log
		cron = config[0].Cron
		handler = config[0].Handler
	}
	if log == nil {
		log = slog.Default()
	}
	if cron == "" {
		cron = "* * * * *"
	} else {
		if _, err := gronx.NextTick(cron, false); err != nil {
			panic(err)
		}
	}
	if handler == nil {
		handler = dummySchedulerHandler2[S]
	}
	meta := task.TaskModelMeta(new(S))
	scheduler := Scheduler2[S]{
		log:     log,
		handler: handler,
		db:      db,
		meta:    meta,
		cron:    cron,
	}
	return &scheduler
}

func (s *Scheduler2[S]) Start() {
	s.StartContext(context.Background())
}

func (s *Scheduler2[S]) StartContext(ctx context.Context) {
	if s.running.Load() {
		panic(errors.New("already running"))
	}

	s.stoped.Add(1)
	ctx, s.cancel = context.WithCancel(ctx)
	go s.run(ctx)
}

func (s *Scheduler2[S]) Stop() {
	s.log.Debug("Stopping")
	s.cancel()
	s.stoped.Wait()
	s.log.Debug("Stopped")
}

func (s *Scheduler2[S]) run(ctx context.Context) {
	s.log.Debug("scheduler stated")
	defer func() {
		s.log.Debug("scheduler terminated")
	}()

	s.running.Store(true)
	defer func() {
		s.running.Store(false)
	}()

	defer s.stoped.Done()

	if nextTs, err := gronx.NextTick(s.cron, true); err != nil {
		panic(err)
	} else {
		s.nextTs = nextTs
	}

	for {
		s.log.Info("next event time", "NextTs", s.nextTs)
		d := time.Until(s.nextTs)
		timer := time.NewTimer(d)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			s.log.Info("terminate")
			return
		case <-timer.C:
			if err := s.processTasks(); err != nil {
				panic(err)
			}

			if nextTs, err := gronx.NextTick(s.cron, false); err != nil {
				panic(err)
			} else {
				s.nextTs = nextTs
			}
		}
	}
}

func (s *Scheduler2[S]) processTasks() error {
	c := &s.meta

	s.log.Info("process")

	err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		schedules, err := s.FindAllActiveAndUnprocessed(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		s.log.Debug("the schedules is loaded", "count", len(schedules))
		for _, dbSchedule := range schedules {
			s.log.Info("process the schedule", "schedule", dbSchedule)

			tv := reflect.ValueOf(dbSchedule).Elem()

			lastRunAt := time.Now().UTC()

			nextRunAtV := tv.FieldByName(c.FieldsByName["NextRunAt"].StructName).Interface()
			var nextRunAt *time.Time
			if val, ok := nextRunAtV.(time.Time); ok {
				nextRunAt = &val
			} else if val, ok := nextRunAtV.(*time.Time); ok {
				nextRunAt = val
			}

			cronV := tv.FieldByName(c.FieldsByName["Cron"].StructName).Interface()
			var cron *string
			if val, ok := cronV.(string); ok {
				cron = &val
			} else if val, ok := cronV.(*string); ok {
				cron = val
			}

			if nextRunAt == nil && cron == nil {
				s.log.Debug("the schedule is not valid")
				// dbSchedule.IsActive = false
				task.SetFieldValue(tv.FieldByName(c.FieldsByName["IsActive"].StructName), false)
				if err := s.Save(tx, dbSchedule); err != nil {
					return err
				}
				continue
			}
			if err := s.handler(tx, dbSchedule); err != nil {
				s.log.Error("the schedule processed with error", "error", err)
			}
			if cron == nil {
				s.log.Info("the schedule is one shot")
				// dbSchedule.IsActive = false
				// dbSchedule.LastRunAt = dbSchedule.NextRunAt
				task.SetFieldValue(tv.FieldByName(c.FieldsByName["IsActive"].StructName), false)
				task.SetFieldValue(tv.FieldByName(c.FieldsByName["LastRunAt"].StructName), lastRunAt)
			} else {
				if nextTs, err := gronx.NextTick(*cron, false); err != nil {
					s.log.Info("the schedule has an invalid cron expression", "error", err)
					// dbSchedule.IsActive = false
					// dbSchedule.LastRunAt = dbSchedule.NextRunAt
					task.SetFieldValue(tv.FieldByName(c.FieldsByName["IsActive"].StructName), false)
					task.SetFieldValue(tv.FieldByName(c.FieldsByName["LastRunAt"].StructName), lastRunAt)
				} else {
					s.log.Info("the schedule is planned", "time", nextTs)
					// dbSchedule.LastRunAt = dbSchedule.NextRunAt
					// dbSchedule.NextRunAt = &nextTs
					task.SetFieldValue(tv.FieldByName(c.FieldsByName["NextRunAt"].StructName), nextTs)
					task.SetFieldValue(tv.FieldByName(c.FieldsByName["LastRunAt"].StructName), lastRunAt)
				}
			}
			if err := s.Save(tx, dbSchedule); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *Scheduler2[S]) CreateTable(tx *sql.Tx) error {
	c := &s.meta
	query := fmt.Sprintf(`
		create table if not exists %s (
			id serial not null, 
			name varchar not null, 
			is_active boolean default true not null, 
			cron varchar, 
			next_run_at timestamp with time zone, 
			last_run_at timestamp with time zone, 
			func varchar not null,
			args jsonb, 
			primary key (id)
		);`,
		c.TableName,
	)
	_, err := tx.Exec(query)
	return err
}

func (s *Scheduler2[S]) FindAllActiveAndUnprocessed(tx *sql.Tx, moment time.Time) ([]*S, error) {
	c := &s.meta

	var fields []string
	for _, f := range c.Fields {
		fields = append(fields, f.DbName)
	}

	query := fmt.Sprintf(
		`select %s  
		from %s
		where %s and (%s is null or %s < $1)
		order by %s
		for update`,
		strings.Join(fields, ", "),
		c.TableName,
		c.FieldsByName["IsActive"].DbName, c.FieldsByName["NextRunAt"].DbName, c.FieldsByName["NextRunAt"].DbName,
		c.FieldsByName["NextRunAt"].DbName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(moment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*S
	for rows.Next() {
		values := make([]any, len(fields))
		valuesHolder := make([]any, len(fields))
		for i := range values {
			valuesHolder[i] = &values[i]
		}

		err := rows.Scan(valuesHolder...)
		if err != nil {
			return nil, err
		}
		var schedule = new(S)
		v := reflect.ValueOf(schedule).Elem()
		for pos, f := range c.Fields {
			value := values[pos]
			if f.Name == "Args" {
				bytes := value.([]byte)
				if err := json.Unmarshal(bytes, &value); err != nil {
					return nil, err
				}
			}
			task.SetFieldValue(v.FieldByName(f.StructName), value)
		}
		schedules = append(schedules, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

func (s *Scheduler2[S]) Create(tx *sql.Tx, schedule *S) error {
	c := &s.meta

	tv := reflect.ValueOf(schedule).Elem()

	var fields []string
	var valuesHolder []string
	var values []any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			continue
		}
		value := tv.FieldByName(f.StructName).Interface()
		if f.Name == "Args" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}
		fields = append(fields, f.DbName)
		valuesHolder = append(valuesHolder, fmt.Sprintf("$%d", idx))
		values = append(values, value)
		idx++
	}

	// query
	query := fmt.Sprintf(
		`insert into %s(%s) 
		values (%s) 
		returning %s`,
		c.TableName,
		strings.Join(fields, ", "),
		strings.Join(valuesHolder, ", "),
		c.FieldsByName["Id"].DbName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id any
	err = stmt.QueryRow(values...).Scan(&id)
	task.SetFieldValue(tv.FieldByName(c.FieldsByName["Id"].StructName), id)

	return err
}

func (s *Scheduler2[S]) Save(tx *sql.Tx, schedule *S) error {
	c := &s.meta

	tv := reflect.ValueOf(schedule).Elem()

	var idValue any
	var fields []string
	var values []any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			idValue = tv.FieldByName(f.StructName).Interface()
			continue
		}

		value := tv.FieldByName(f.StructName).Interface()

		if f.Name == "Args" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}

		fields = append(fields, fmt.Sprintf("%s=$%d", f.DbName, idx))
		values = append(values, value)
		idx++
	}

	values = append(values, idValue)
	query := fmt.Sprintf(
		`update %s
		set %s
		where %s=$%d`,
		c.TableName,
		strings.Join(fields, ", "),
		c.FieldsByName["Id"].DbName, len(values),
	)
	res, err := tx.Exec(query, values...)
	if err != nil {
		return err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		return err
	} else {
		if rowsAffected == 0 {
			return sql.ErrNoRows
		}
	}
	return nil
}

func (s *Scheduler2[S]) Delete(tx *sql.Tx, pk any) error {
	c := &s.meta
	query := fmt.Sprintf(
		`delete from %s where %s=$1`,
		c.TableName, c.FieldsByName["Id"].DbName,
	)
	res, err := tx.Exec(query, pk)
	if err != nil {
		return err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		return err
	} else {
		if rowsAffected == 0 {
			return sql.ErrNoRows
		}
	}
	return nil
}

func (s *Scheduler2[S]) DeleteAll(tx *sql.Tx) error {
	c := &s.meta
	query := fmt.Sprintf(
		`delete from %s`,
		c.TableName,
	)
	_, err := tx.Exec(query)
	return err
}
