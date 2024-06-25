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
	Repository SchedulerRepository
	Cron       string
	Handler    SchedulerHandler
}

type SimpleScheduler struct {
	log        *slog.Logger
	handler    SchedulerHandler
	db         *sql.DB
	repository SchedulerRepository
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
		config.Repository = NewDefaultPostgresSchedulerRepository()
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
			limit := 50
			i := 0
			for i < 100 {
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

func (s *SimpleScheduler) processTasks(limit int) (int, error) {
	s.log.Info("process")

	processed := 0
	err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		schedules, err := s.repository.FindActiveAndExpired(tx, nil, limit)
		if err != nil {
			return err
		}
		for _, schedule := range schedules {
			processed += 1
			s.log.Debug("process schedule", "schedule", schedule)
			if schedule.NextTs == nil && schedule.Cron == nil {
				s.log.Info("schedule is not valid", "schedule", schedule.Id)
				schedule.IsActive = false
				if err := s.repository.Save(tx, schedule); err != nil {
					return err
				}
			}
			if err := s.handler(tx, schedule.Name, *schedule.NextTs, schedule.Message); err != nil {
				return err
			}
			if schedule.Cron == nil {
				schedule.IsActive = false
				schedule.LastTs = schedule.NextTs
			} else {
				if nextTs, err := gronx.NextTick(*schedule.Cron, false); err != nil {
					s.log.Info("invalid cron expression", "schedule", schedule.Id, "error", err)
					schedule.IsActive = false
					schedule.LastTs = schedule.NextTs
				} else {
					schedule.LastTs = schedule.NextTs
					schedule.NextTs = &nextTs
				}
			}
			if err := s.repository.Save(tx, schedule); err != nil {
				return err
			}
		}
		return nil
	})
	return processed, err
}
