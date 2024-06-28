package queue

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

type MessageHandler func(tx *sql.Tx, message *Message) error

type WorkerConfig struct {
	Log        *slog.Logger
	Repository QueueRepository
	Cron       string
	Handler    MessageHandler
}

type Worker struct {
	log        *slog.Logger
	handler    MessageHandler
	db         *sql.DB
	repository QueueRepository
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
		config.Repository = NewPostgresQueueRepository()
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
		config.Handler = dummyMessageHandler
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

	if err := w.process(); err != nil {
		panic(err)
	}

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
		}
	}
}

func (w *Worker) process() error {
	w.log.Debug("process")
	for {
		err := barngo.RunInTransaction(w.db, func(tx *sql.Tx) error {
			message, err := w.repository.FindNext(tx)
			if err != nil {
				return err
			}
			if message == nil {
				return sql.ErrNoRows
			}
			w.log.Info("process message", "message", message)
			if err := w.handler(tx, message); err != nil {
				w.log.Error("the message is processed with error", "error", err)
			} else {
				w.log.Error("the message is processed")
			}
			w.log.Debug("save message", "message", message)
			if err := w.repository.Delete(tx, message); err != nil {
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

func dummyMessageHandler(tx *sql.Tx, message *Message) error {
	slog.Info("DUMMY: process", "message", message)
	return nil
}
