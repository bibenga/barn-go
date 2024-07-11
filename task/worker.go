package task

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
	barngo "github.com/bibenga/barn-go"
)

type TaskHandler func(tx *sql.Tx, task *Task) error

type WorkerConfig struct {
	Log        *slog.Logger
	Repository TaskRepository
	Cron       string
	Handler    TaskHandler
}

type Worker struct {
	log        *slog.Logger
	handler    TaskHandler
	db         *sql.DB
	repository TaskRepository
	cron       string
	running    atomic.Bool
	cancel     context.CancelFunc
	stoped     sync.WaitGroup
}

func NewWorker(db *sql.DB, config *WorkerConfig) *Worker {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Repository == nil {
		config.Repository = NewPostgresTaskRepository()
		// or just panic?
		// panic(errors.New("repository is nil"))
	}
	if config.Cron == "" {
		config.Cron = "* * * * *"
	} else {
		if _, err := gronx.NextTick(config.Cron, false); err != nil {
			panic(err)
		}
	}
	if config.Handler == nil {
		config.Handler = dummyTaskHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	w := Worker{
		log:        config.Log,
		handler:    config.Handler,
		db:         db,
		cron:       config.Cron,
		repository: config.Repository,
	}
	return &w
}

func (w *Worker) Start() {
	w.StartContext(context.Background())
}

func (w *Worker) StartContext(ctx context.Context) {
	if w.running.Load() {
		panic(errors.New("already running"))
	}

	w.stoped.Add(1)
	ctx, w.cancel = context.WithCancel(ctx)
	go w.run(ctx)
}

func (w *Worker) Stop() {
	w.log.Debug("Stopping")
	w.cancel()
	w.stoped.Wait()
	w.log.Debug("Stopped")
}

func (w *Worker) run(ctx context.Context) {
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

func (w *Worker) process() error {
	w.log.Debug("process")
	for {
		err := barngo.RunInTransaction(w.db, func(tx *sql.Tx) error {
			t, err := w.repository.FindNext(tx)
			if err != nil {
				return err
			}
			if t == nil {
				return sql.ErrNoRows
			}
			w.log.Info("process task", "task", t)
			if t.Status != Queued {
				return errors.New("codebug: task is processed")
			}
			startedAt := time.Now().UTC()
			if err := w.handler(tx, t); err != nil {
				w.log.Error("the task is processed with error", "error", err)
				finishedAt := time.Now().UTC()
				errorMessage := err.Error()
				t.Status = Failed
				t.StartedAt = &startedAt
				t.FinishedAt = &finishedAt
				t.Error = &errorMessage
			} else {
				w.log.Info("the task is processed with success")
				finishedAt := time.Now().UTC()
				t.Status = Done
				t.StartedAt = &startedAt
				t.FinishedAt = &finishedAt
				// task.Result = nil
			}
			w.log.Debug("save task", "task", t)
			if err := w.repository.Save(tx, t); err != nil {
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

func (w *Worker) deleteOld() error {
	w.log.Debug("deleteOld")
	return barngo.RunInTransaction(w.db, func(tx *sql.Tx) error {
		m := time.Now().UTC().Add(-30 * 24 * time.Hour)
		deleted, err := w.repository.DeleteOld(tx, m)
		w.log.Debug("the old tasks is deleted", "count", deleted)
		return err
	})
}

func dummyTaskHandler[T any](tx *sql.Tx, t *T) error {
	slog.Info("DUMMY: process", "task", t)
	return nil
}
