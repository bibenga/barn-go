package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
)

type SimpleSchedulerHandler func(tx *sql.Tx, name string, moment time.Time, message *string) error

type SimpleSchedulerConfig struct {
	Log     *slog.Logger
	Query   *SimpleTaskQuery
	Cron    string
	Handler SimpleSchedulerHandler
}

type SimpleScheduler struct {
	log     *slog.Logger
	handler SimpleSchedulerHandler
	db      *sql.DB
	query   *SimpleTaskQuery
	running atomic.Bool
	cancel  context.CancelFunc
	stoped  sync.WaitGroup
	cron    string
	nextTs  time.Time
}

func NewSimpleScheduler(db *sql.DB, config *SimpleSchedulerConfig) *SimpleScheduler {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Query == nil {
		config.Query = NewDefaultSimpleTaskQuery()
	}
	if config.Cron == "" {
		config.Cron = "* * * * *"
	}
	if config.Handler == nil {
		config.Handler = dummySimpleSchedulerHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := SimpleScheduler{
		log:     config.Log,
		handler: config.Handler,
		db:      db,
		query:   config.Query,
		cron:    config.Cron,
	}
	return &scheduler
}

func (s *SimpleScheduler) CreateTable() error {
	s.log.Info("create table")
	_, err := s.db.Exec(s.query.CreateTableQuery)
	return err
}

func (s *SimpleScheduler) Start() {
	s.StartContext(context.Background())
}

func (s *SimpleScheduler) StartContext(ctx context.Context) {
	if s.running.Load() {
		panic(errors.New("already running"))
	}

	s.stoped.Add(1)
	ctx, s.cancel = context.WithCancel(ctx)
	go s.run(ctx)
}

func (s *SimpleScheduler) Stop() {
	if !s.running.Load() {
		panic(errors.New("is not running"))
	}

	s.log.Debug("Stopping")
	s.cancel()
	s.stoped.Wait()
	s.log.Debug("Stopped")
}

func (s *SimpleScheduler) run(ctx context.Context) {
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
			if err := s.initTasks(); err != nil {
				panic(err)
			}

			limit := 50
			i := 0
			for i < 10 {
				if processed, err := s.processTasks(limit); err != nil {
					panic(err)
				} else {
					if processed < limit {
						break
					}
				}
			}

			if nextTs, err := gronx.NextTick(s.cron, false); err != nil {
				panic(err)
			} else {
				s.nextTs = nextTs
			}
		}
	}
}

func (s *SimpleScheduler) initTasks() error {
	s.log.Info("initTasks")

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(s.query.SelectForInitQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return err
	}

	processed := 0
	for rows.Next() {
		processed += 1
		var t Task = Task{}
		err := rows.Scan(&t.Id, &t.Name, &t.IsActive, &t.Cron, &t.NextTs, &t.LastTs, &t.Message)
		if err != nil {
			return err
		}
		s.log.Info("process task", "task", t)
		if t.IsActive {
			if t.Cron == nil && t.NextTs == nil {
				s.log.Warn("invalid task", "task", t)
				s.deactivate(tx, t)
			} else if t.Cron != nil && t.NextTs == nil {
				if nextTs, err := gronx.NextTick(*t.Cron, true); err != nil {
					return err
				} else {
					t.NextTs = &nextTs
					s.update(tx, &t)
				}
			}
		} else {
			s.log.Info("the task is inactive", "task", t)
		}
	}
	s.log.Info("processed", "count", processed)

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *SimpleScheduler) processTasks(limit int) (int, error) {
	s.log.Info("process")

	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(s.query.SelectForProcessQuery)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(limit)
	if err != nil {
		return 0, err
	}
	processed := 0
	for rows.Next() {
		processed += 1
		var t Task = Task{}
		err := rows.Scan(&t.Id, &t.Name, &t.IsActive, &t.Cron, &t.NextTs, &t.LastTs, &t.Message)
		if err != nil {
			return 0, err
		}
		if t.IsActive {
			if err := s.handler(tx, t.Name, *t.NextTs, t.Message); err != nil {
				return 0, err
			}

			if t.Cron == nil && t.NextTs == nil {
				s.log.Warn("invalid task", "task", t)
				s.deactivate(tx, t)
			} else if t.Cron == nil {
				s.deactivate(tx, t)
			} else {
				if nextTs, err := gronx.NextTick(*t.Cron, false); err != nil {
					return 0, err
				} else {
					t.NextTs = &nextTs
					s.update(tx, &t)
				}
			}
		} else {
			s.log.Info("the task is inactive", "task", t)
		}
	}
	s.log.Info("processed", "count", processed)

	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return processed, nil
}

func (s *SimpleScheduler) update(tx *sql.Tx, task *Task) error {
	s.log.Info("update the task", "task", task)
	res, err := tx.Exec(
		s.query.UpdateQuery,
		task.IsActive, task.NextTs, task.LastTs, task.Id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("we inside select for update, what's happened?")
	}
	return nil
}

func (s *SimpleScheduler) deactivate(tx *sql.Tx, task Task) error {
	task.IsActive = false
	s.log.Info("deactivate the task", "task", task)
	res, err := tx.Exec(
		s.query.UpdateIsActiveQuery,
		false, task.Id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return fmt.Errorf("we inside select for update, what's happened?")
	}
	return nil
}

func dummySimpleSchedulerHandler(tx *sql.Tx, name string, moment time.Time, message *string) error {
	return dummySchedulerHandler(nil, name, moment, message)
}
