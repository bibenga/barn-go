package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
)

type SimpleSchedulerConfig struct {
	Log      *slog.Logger
	Query    *TaskQuery
	Cron     string
	Listener SchedulerListener
}

type SimpleScheduler struct {
	log      *slog.Logger
	listener SchedulerListener
	db       *sql.DB
	query    *TaskQuery
	running  atomic.Bool
	cancel   context.CancelFunc
	stoped   sync.WaitGroup
	cron     string
	nextTs   time.Time
}

func NewSimpleScheduler(db *sql.DB, config *SimpleSchedulerConfig) *SimpleScheduler {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Query == nil {
		query := NewDefaultTaskQuery()
		config.Query = &query
	}
	if config.Cron == "" {
		config.Cron = "* * * * *"
	}
	if config.Listener == nil {
		config.Listener = &DummySchedulerListener{}
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := SimpleScheduler{
		log:      config.Log,
		listener: config.Listener,
		db:       db,
		query:    config.Query,
		cron:     config.Cron,
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

	s.initNextTs()

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
			if err := s.process(); err != nil {
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

func (s *SimpleScheduler) initNextTs() error {
	s.log.Info("process")

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *SimpleScheduler) process() error {
	s.log.Info("process")

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}
