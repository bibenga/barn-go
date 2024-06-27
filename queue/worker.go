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
	Repository MessageRepository
	Cron       string
	Handler    MessageHandler
}

type Worker struct {
	log        *slog.Logger
	handler    MessageHandler
	db         *sql.DB
	repository MessageRepository
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
		config.Repository = NewDefaultPostgresMessageRepository()
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
	worker := Worker{
		log:        config.Log,
		handler:    config.Handler,
		db:         db,
		cron:       config.Cron,
		repository: config.Repository,
	}
	return &worker
}

func (s *Worker) Start() {
	s.StartContext(context.Background())
}

func (s *Worker) StartContext(ctx context.Context) {
	if s.running.Load() {
		panic(errors.New("already running"))
	}

	s.stoped.Add(1)
	ctx, s.cancel = context.WithCancel(ctx)
	go s.run(ctx)
}

func (s *Worker) Stop() {
	s.log.Debug("Stopping")
	s.cancel()
	s.stoped.Wait()
	s.log.Debug("Stopped")
}

func (s *Worker) run(ctx context.Context) {
	s.log.Debug("worker is stated")
	defer func() {
		s.log.Debug("worker is stopped")
	}()

	s.running.Store(true)
	defer func() {
		s.running.Store(false)
	}()

	defer s.stoped.Done()

	s.process()

	for {
		nextTs, err := gronx.NextTick(s.cron, false)
		if err != nil {
			panic(err)
		}
		d := time.Until(nextTs)
		s.log.Debug("next fire time", "time", nextTs, "duration", d)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			s.log.Debug("terminate")
			return
		case <-timer.C:
			if err := s.process(); err != nil {
				panic(err)
			}
			if err := s.deleteOld(); err != nil {
				panic(err)
			}
		}
	}
}

func (s *Worker) process() error {
	s.log.Debug("process")
	for {
		err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
			message, err := s.repository.FindNext(tx)
			if err != nil {
				return err
			}
			if message == nil {
				return sql.ErrNoRows
			}
			s.log.Info("process message", "message", message)
			if message.IsProcessed {
				return errors.New("codebug: message is processed")
			}
			if err := s.handler(tx, message); err != nil {
				s.log.Error("the message is processed with error", "error", err)
				now := time.Now().UTC()
				success := false
				message.IsProcessed = true
				message.ProcessedAt = &now
				message.IsSuccess = &success
				errorMessage := err.Error()
				message.Error = &errorMessage
			} else {
				now := time.Now().UTC()
				success := true
				message.IsProcessed = true
				message.ProcessedAt = &now
				message.IsSuccess = &success
			}
			s.log.Debug("save message", "message", message)
			if err := s.repository.Save(tx, message); err != nil {
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

func (s *Worker) deleteOld() error {
	s.log.Debug("deleteOld")
	return barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		m := time.Now().UTC().Add(-30 * 24 * time.Hour)
		deleted, err := s.repository.DeleteProcessed(tx, m)
		s.log.Debug("the old messages is deleted", "count", deleted)
		return err
	})
}

func dummyMessageHandler(tx *sql.Tx, message *Message) error {
	slog.Info("DUMMY: process", "message", message)
	return nil
}
