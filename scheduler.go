package barngo

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
)

type SchedulerHandler[S any] func(tx *sql.Tx, schedule *S) error

type SchedulerConfig[S any] struct {
	Log     *slog.Logger
	Cron    string
	Handler SchedulerHandler[S]
}

type Scheduler[S any] struct {
	log     *slog.Logger
	handler SchedulerHandler[S]
	db      *sql.DB
	meta    TableMeta
	running atomic.Bool
	cancel  context.CancelFunc
	stoped  sync.WaitGroup
	cron    string
	nextTs  time.Time
}

func NewScheduler[S any](db *sql.DB, config ...SchedulerConfig[S]) *Scheduler[S] {
	var log *slog.Logger
	var cron string
	var handler SchedulerHandler[S]
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
		handler = dummySchedulerHandler[S]
	}

	meta := GetTableMeta(new(S))
	// TODO: check required fields

	scheduler := Scheduler[S]{
		log:     log,
		handler: handler,
		db:      db,
		meta:    meta,
		cron:    cron,
	}
	return &scheduler
}

func (w *Scheduler[S]) Start() {
	w.StartContext(context.Background())
}

func (w *Scheduler[S]) StartContext(ctx context.Context) {
	if w.running.Load() {
		panic(errors.New("already running"))
	}

	w.stoped.Add(1)
	ctx, w.cancel = context.WithCancel(ctx)
	go w.run(ctx)
}

func (w *Scheduler[S]) Stop() {
	w.log.Debug("Stopping")
	w.cancel()
	w.stoped.Wait()
	w.log.Debug("Stopped")
}

func (w *Scheduler[S]) run(ctx context.Context) {
	w.log.Debug("scheduler stated")
	defer func() {
		w.log.Debug("scheduler terminated")
	}()

	w.running.Store(true)
	defer func() {
		w.running.Store(false)
	}()

	defer w.stoped.Done()

	if nextTs, err := gronx.NextTick(w.cron, true); err != nil {
		panic(err)
	} else {
		w.nextTs = nextTs
	}

	for {
		w.log.Info("next event time", "NextTs", w.nextTs)
		d := time.Until(w.nextTs)
		timer := time.NewTimer(d)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			w.log.Info("terminate")
			return
		case <-timer.C:
			if err := w.processTasks(); err != nil {
				panic(err)
			}

			if nextTs, err := gronx.NextTick(w.cron, false); err != nil {
				panic(err)
			} else {
				w.nextTs = nextTs
			}
		}
	}
}

func (w *Scheduler[S]) processTasks() error {
	w.log.Info("process")
	meta := &w.meta

	err := RunInTransaction(w.db, func(tx *sql.Tx) error {
		schedules, err := w.FindAllActive(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		w.log.Debug("the schedules is loaded", "count", len(schedules))
		for _, s := range schedules {
			w.log.Info("process the schedule", "schedule", s)

			sv := reflect.ValueOf(s).Elem()

			if err := w.handler(tx, s); err != nil {
				w.log.Error("the schedule processed with error", "error", err)
			}

			meta.SetValue(sv, "LastRunAt", time.Now().UTC())

			var nextRunAt *time.Time
			nextRunAtV, _ := meta.GetValue(sv, "NextRunAt")
			if val, ok := nextRunAtV.(time.Time); ok {
				nextRunAt = &val
			} else if val, ok := nextRunAtV.(*time.Time); ok {
				nextRunAt = val
			}

			var interval *time.Duration
			if intervalV, ok := meta.GetValue(sv, "Interval"); ok {
				if val, ok := intervalV.(time.Duration); ok {
					interval = &val
				} else if val, ok := intervalV.(*time.Duration); ok {
					interval = val
				}
			}

			var cron *string
			if cronV, ok := meta.GetValue(sv, "Cron"); ok {
				if val, ok := cronV.(string); ok {
					cron = &val
				} else if val, ok := cronV.(*string); ok {
					cron = val
				}
			}

			if interval != nil {
				var nextTs time.Time
				if nextRunAt == nil {
					nextTs = time.Now().UTC().Add(*interval)
				} else {
					nextTs = nextRunAt.Add(*interval)
				}
				w.log.Info("the schedule is planned", "time", nextTs)
				meta.SetValue(sv, "NextRunAt", nextTs)
			} else if cron != nil {
				if nextTs, err := gronx.NextTick(*cron, false); err != nil {
					w.log.Warn("the schedule has an invalid cron expression", "error", err)
					meta.SetValue(sv, "IsActive", false)
				} else {
					w.log.Info("the schedule is planned", "time", nextTs)
					meta.SetValue(sv, "NextRunAt", nextTs)
				}
			} else {
				w.log.Info("the schedule is one shot")
				meta.SetValue(sv, "IsActive", false)
			}

			if err := w.Save(tx, s); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (w *Scheduler[S]) FindAllActive(tx *sql.Tx, moment time.Time) ([]*S, error) {
	meta := &w.meta

	var fields []string
	for _, f := range meta.Fields {
		fields = append(fields, f.ColumnName)
	}

	query := fmt.Sprintf(
		`select %s  
		from %s
		where %s and (%s is null or %s < $1)
		order by %s
		for update`,
		strings.Join(fields, ", "),
		meta.TableName,
		meta.FieldsByName["IsActive"].ColumnName, meta.FieldsByName["NextRunAt"].ColumnName, meta.FieldsByName["NextRunAt"].ColumnName,
		meta.FieldsByName["NextRunAt"].ColumnName,
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
		for pos, f := range meta.Fields {
			value := values[pos]
			if f.Name == "Args" {
				bytes := value.([]byte)
				if err := json.Unmarshal(bytes, &value); err != nil {
					return nil, err
				}
			}
			meta.SetValue(v, f.Name, value)
		}
		schedules = append(schedules, schedule)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

func (w *Scheduler[S]) Create(tx *sql.Tx, s *S) error {
	meta := &w.meta

	sv := reflect.ValueOf(s).Elem()

	var fields []string
	var valuesHolder []string
	var values []any
	idx := 1
	for _, f := range meta.Fields {
		if f.Name == "Id" {
			continue
		}
		value, _ := meta.GetValue(sv, f.Name)
		if f.Name == "Args" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}
		fields = append(fields, f.ColumnName)
		valuesHolder = append(valuesHolder, fmt.Sprintf("$%d", idx))
		values = append(values, value)
		idx++
	}

	// query
	query := fmt.Sprintf(
		`insert into %s(%s) 
		values (%s) 
		returning %s`,
		meta.TableName,
		strings.Join(fields, ", "),
		strings.Join(valuesHolder, ", "),
		meta.FieldsByName["Id"].ColumnName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id any
	err = stmt.QueryRow(values...).Scan(&id)
	meta.SetValue(sv, "Id", id)

	return err
}

func (w *Scheduler[S]) Save(tx *sql.Tx, s *S) error {
	meta := &w.meta

	sv := reflect.ValueOf(s).Elem()

	var idValue any
	var fields []string
	var values []any
	idx := 1
	for _, f := range meta.Fields {
		if f.Name == "Id" {
			idValue, _ = meta.GetValue(sv, "Id")
			continue
		}

		value, _ := meta.GetValue(sv, f.Name)
		if f.Name == "Args" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}

		fields = append(fields, fmt.Sprintf("%s=$%d", f.ColumnName, idx))
		values = append(values, value)
		idx++
	}

	values = append(values, idValue)
	query := fmt.Sprintf(
		`update %s
		set %s
		where %s=$%d`,
		meta.TableName,
		strings.Join(fields, ", "),
		meta.FieldsByName["Id"].ColumnName, len(values),
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

func (w *Scheduler[S]) Delete(tx *sql.Tx, pk any) error {
	meta := &w.meta
	query := fmt.Sprintf(
		`delete from %s where %s=$1`,
		meta.TableName, meta.FieldsByName["Id"].ColumnName,
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

func (w *Scheduler[S]) DeleteAll(tx *sql.Tx) error {
	meta := &w.meta
	query := fmt.Sprintf(
		`delete from %s`,
		meta.TableName,
	)
	_, err := tx.Exec(query)
	return err
}

func dummySchedulerHandler[S any](tx *sql.Tx, s *S) error {
	slog.Debug("DUMMY: process", "schedule", s)
	return nil
}

var _ SchedulerHandler[Schedule] = dummySchedulerHandler[Schedule]

func DefaultSchedulerHandler(worker *Worker[Task]) SchedulerHandler[Schedule] {
	handler := func(tx *sql.Tx, schedule *Schedule) error {
		task := Task{
			Func: schedule.Func,
			Args: schedule.Args,
		}
		return worker.Create(tx, &task)
	}
	return handler
}
