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
	barngo "github.com/bibenga/barn-go"
)

type SimpleSchedulerConfig struct {
	Log        *slog.Logger
	Repository SimpleSchedulerRepository
	Cron       string
	Handler    SchedulerHandler
}

type SimpleScheduler struct {
	log        *slog.Logger
	handler    SchedulerHandler
	db         *sql.DB
	repository SimpleSchedulerRepository
	running    atomic.Bool
	cancel     context.CancelFunc
	stoped     sync.WaitGroup
	cron       string
	nextTs     time.Time
}

func NewSimpleScheduler(db *sql.DB, config *SimpleSchedulerConfig) *SimpleScheduler {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Repository == nil {
		config.Repository = NewDefaultPostgresSimpleSchedulerRepository()
	}
	if config.Cron == "" {
		config.Cron = "* * * * *"
	} else {
		if _, err := gronx.NextTick(config.Cron, false); err != nil {
			panic(err)
		}
	}
	if config.Handler == nil {
		config.Handler = dummySchedulerHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := SimpleScheduler{
		log:        config.Log,
		handler:    config.Handler,
		db:         db,
		repository: config.Repository,
		cron:       config.Cron,
	}
	return &scheduler
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

func (s *SimpleScheduler) processTasks() error {
	s.log.Info("process")

	err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		schedules, err := s.repository.FindAllActiveAndUnprocessed(tx, time.Now().UTC())
		if err != nil {
			return err
		}
		s.log.Debug("the schedules is loaded", "count", len(schedules))
		for _, dbSchedule := range schedules {
			s.log.Info("process the schedule", "schedule", dbSchedule)
			if dbSchedule.NextRun == nil && dbSchedule.Cron == nil {
				s.log.Debug("the schedule is not valid")
				dbSchedule.IsActive = false
				if err := s.repository.Save(tx, dbSchedule); err != nil {
					return err
				}
				continue
			}
			if err := s.handler(tx, dbSchedule.Name, *dbSchedule.NextRun, dbSchedule.Message); err != nil {
				s.log.Error("the schedule processed with error", "error", err)
			}
			if dbSchedule.Cron == nil {
				s.log.Info("the schedule is one shot")
				dbSchedule.IsActive = false
				dbSchedule.LastRun = dbSchedule.NextRun
			} else {
				if nextTs, err := gronx.NextTick(*dbSchedule.Cron, false); err != nil {
					s.log.Info("the schedule has an invalid cron expression", "error", err)
					dbSchedule.IsActive = false
					dbSchedule.LastRun = dbSchedule.NextRun
				} else {
					s.log.Info("the schedule is planned", "time", nextTs)
					dbSchedule.LastRun = dbSchedule.NextRun
					dbSchedule.NextRun = &nextTs
				}
			}
			if err := s.repository.Save(tx, dbSchedule); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
