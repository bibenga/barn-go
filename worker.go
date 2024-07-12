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

const IgnoreResult = "IgnoreResult"

type TaskHandler[T any] func(tx *sql.Tx, task *T) (any, error)

type WorkerConfig[T any] struct {
	Log     *slog.Logger
	Cron    string
	Handler TaskHandler[T]
}

type Worker[T any] struct {
	log     *slog.Logger
	handler TaskHandler[T]
	db      *sql.DB
	meta    TableMeta
	cron    string
	running atomic.Bool
	cancel  context.CancelFunc
	stoped  sync.WaitGroup
}

func NewWorker[T any](db *sql.DB, config ...WorkerConfig[T]) *Worker[T] {
	if db == nil {
		panic(errors.New("db is nil"))
	}

	var log *slog.Logger
	var cron string
	var handler TaskHandler[T]
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
		handler = dummyTaskHandler[T]
	}
	meta := GetTableMeta(new(T))
	w := Worker[T]{
		log:     log,
		handler: handler,
		db:      db,
		meta:    meta,
		cron:    cron,
	}
	return &w
}

func (w *Worker[T]) Start() {
	w.StartContext(context.Background())
}

func (w *Worker[T]) StartContext(ctx context.Context) {
	if w.running.Load() {
		panic(errors.New("already running"))
	}

	w.stoped.Add(1)
	ctx, w.cancel = context.WithCancel(ctx)
	go w.run(ctx)
}

func (w *Worker[T]) Stop() {
	w.log.Debug("Stopping")
	w.cancel()
	w.stoped.Wait()
	w.log.Debug("Stopped")
}

func (w *Worker[T]) run(ctx context.Context) {
	w.log.Debug("worker is stated")
	defer func() {
		w.log.Debug("worker is stopped")
	}()

	w.running.Store(true)
	defer func() {
		w.running.Store(false)
	}()

	defer w.stoped.Done()

	w.process()

	for {
		nextTs, err := gronx.NextTick(w.cron, false)
		if err != nil {
			panic(err)
		}
		d := time.Until(nextTs)
		w.log.Debug("next fire time", "time", nextTs, "duration", d)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			w.log.Debug("terminate")
			return
		case <-timer.C:
			if err := w.process(); err != nil {
				panic(err)
			}
			if err := w.deleteOld(); err != nil {
				panic(err)
			}
		}
	}
}

func (w *Worker[T]) process() error {
	w.log.Debug("process")
	c := &w.meta
	for {
		err := RunInTransaction(w.db, func(tx *sql.Tx) error {
			t, err := w.FindQueued(tx)
			if err != nil {
				return err
			}
			if t == nil {
				return sql.ErrNoRows
			}
			w.log.Info("process task", "task", t)
			tv := reflect.ValueOf(t).Elem()
			startedAt := time.Now().UTC()
			if result, err := w.handler(tx, t); err != nil {
				w.log.Error("the task is processed with error", "error", err)
				finishedAt := time.Now().UTC()
				errorMessage := err.Error()
				SetFieldValue(tv.FieldByName(c.FieldsByName["Status"].AttrName), Failed)
				SetFieldValue(tv.FieldByName(c.FieldsByName["StartedAt"].AttrName), startedAt)
				SetFieldValue(tv.FieldByName(c.FieldsByName["FinishedAt"].AttrName), finishedAt)
				SetFieldValue(tv.FieldByName(c.FieldsByName["Error"].AttrName), errorMessage)
			} else {
				w.log.Info("the task is processed with success")
				finishedAt := time.Now().UTC()
				SetFieldValue(tv.FieldByName(c.FieldsByName["Status"].AttrName), Done)
				SetFieldValue(tv.FieldByName(c.FieldsByName["StartedAt"].AttrName), startedAt)
				SetFieldValue(tv.FieldByName(c.FieldsByName["FinishedAt"].AttrName), finishedAt)
				if f, ok := c.FieldsByName["Result"]; ok {
					if result != IgnoreResult {
						SetFieldValue(tv.FieldByName(f.AttrName), result)
					}
				}
			}
			w.log.Debug("save task", "task", t)
			if err := w.Save(tx, t); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
	}
}

func (w *Worker[T]) deleteOld() error {
	w.log.Debug("deleteOld")
	return RunInTransaction(w.db, func(tx *sql.Tx) error {
		m := time.Now().UTC().Add(-30 * 24 * time.Hour)
		deleted, err := w.DeleteOld(tx, m)
		w.log.Debug("the old tasks is deleted", "count", deleted)
		return err
	})
}

func (w *Worker[T]) FindQueued(tx *sql.Tx) (*T, error) {
	c := &w.meta

	var fields []string
	for _, f := range c.Fields {
		fields = append(fields, f.ColumnName)
	}

	query := fmt.Sprintf(
		`select %s
		from %s
		where %s = $1 and %s < $2
		order by %s
		limit 1
		for update skip locked`,
		strings.Join(fields, ", "),
		c.TableName,
		c.FieldsByName["Status"].ColumnName, c.FieldsByName["RunAt"].ColumnName,
		c.FieldsByName["RunAt"].ColumnName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(Queued, time.Now().UTC())
	values := make([]any, len(fields))
	valuesHolder := make([]any, len(fields))
	for i := range values {
		valuesHolder[i] = &values[i]
	}
	if err := row.Scan(valuesHolder...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}

	var t = new(T)
	v := reflect.ValueOf(t).Elem()
	for pos, f := range c.Fields {
		value := values[pos]
		if f.Name == "Args" || f.Name == "Result" {
			bytes := value.([]byte)
			if err := json.Unmarshal(bytes, &value); err != nil {
				return nil, err
			}
		}
		SetFieldValue(v.FieldByName(f.AttrName), value)
	}

	return t, nil
}

func (w *Worker[T]) Create(tx *sql.Tx, t *T) error {
	c := &w.meta

	tv := reflect.ValueOf(t).Elem()

	var fields []string
	var valuesHolder []string
	var values []any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			continue
		}

		value := tv.FieldByName(f.AttrName).Interface()

		if f.Name == "RunAt" {
			if v, ok := value.(time.Time); ok {
				if v.IsZero() {
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			} else if v, ok := value.(*time.Time); ok {
				if v == nil {
					// v1 := time.Now().UTC()
					// value = &v1
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			}
		} else if f.Name == "Status" {
			if v, ok := value.(Status); ok {
				if v == "" {
					value = Queued
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			} else {
				value = Queued
				SetFieldValue(tv.FieldByName(f.AttrName), value)
			}

		} else if f.Name == "Args" || f.Name == "Result" {
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
		c.TableName,
		strings.Join(fields, ", "),
		strings.Join(valuesHolder, ", "),
		c.FieldsByName["Id"].ColumnName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id any
	err = stmt.QueryRow(values...).Scan(&id)
	SetFieldValue(tv.FieldByName(c.FieldsByName["Id"].AttrName), id)

	return err
}

func (w *Worker[T]) Save(tx *sql.Tx, t *T) error {
	c := &w.meta

	tv := reflect.ValueOf(t).Elem()

	var idValue any
	var fields []string
	var values []any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			idValue = tv.FieldByName(f.AttrName).Interface()
			continue
		}

		value := tv.FieldByName(f.AttrName).Interface()

		if f.Name == "RunAt" {
			if v, ok := value.(time.Time); ok {
				if v.IsZero() {
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			} else if v, ok := value.(*time.Time); ok {
				if v == nil {
					// v1 := time.Now().UTC()
					// value = &v1
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			}
		} else if f.Name == "Status" {
			if v, ok := value.(Status); ok {
				if v == "" {
					value = Queued
					SetFieldValue(tv.FieldByName(f.AttrName), value)
				}
			} else {
				value = Queued
				SetFieldValue(tv.FieldByName(f.AttrName), value)
			}

		} else if f.Name == "Args" || f.Name == "Result" {
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
		c.TableName,
		strings.Join(fields, ", "),
		c.FieldsByName["Id"].ColumnName, len(values),
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

func (w *Worker[T]) DeleteOld(tx *sql.Tx, moment time.Time) (int, error) {
	c := &w.meta
	query := fmt.Sprintf(
		`delete from %s 
		where %s in ($1, $2) and %s<=$3`,
		c.TableName,
		c.FieldsByName["Status"].ColumnName, c.FieldsByName["RunAt"].ColumnName,
	)
	res, err := tx.Exec(query, Done, Failed, moment)
	if err != nil {
		return 0, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else {
		return int(rowsAffected), nil
	}
}

func (w *Worker[T]) DeleteAll(tx *sql.Tx) error {
	c := &w.meta
	query := fmt.Sprintf(
		`delete from %s`,
		c.TableName,
	)
	_, err := tx.Exec(query)
	return err
}

func dummyTaskHandler[T any](tx *sql.Tx, t *T) (any, error) {
	slog.Info("DUMMY: process", "task", t)
	return IgnoreResult, nil
}

var _ TaskHandler[Task] = dummyTaskHandler[Task]
