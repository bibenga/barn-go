package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
	barngo "github.com/bibenga/barn-go"
)

type SchedulerHandler func(tx *sql.Tx, schedule *Schedule) error

type SchedulerConfig struct {
	Log        *slog.Logger
	Repository SchedulerRepository
	ReloadCron string
	Handler    SchedulerHandler
}

type Scheduler struct {
	log            *slog.Logger
	handler        SchedulerHandler
	db             *sql.DB
	repository     SchedulerRepository
	schedules      map[int]*Schedule
	reloadSchedule Schedule
	running        atomic.Bool
	cancel         context.CancelFunc
	stoped         sync.WaitGroup
}

func NewScheduler(db *sql.DB, config *SchedulerConfig) *Scheduler {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Repository == nil {
		config.Repository = NewPostgresSchedulerRepository()
		// or just panic?
		// panic(errors.New("repository is nil"))
	}
	if config.ReloadCron == "" {
		config.ReloadCron = "*/5 * * * *"
	} else {
		if _, err := gronx.NextTick(config.ReloadCron, false); err != nil {
			panic(err)
		}
	}
	if config.Handler == nil {
		config.Handler = dummySchedulerHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := Scheduler{
		log:        config.Log,
		handler:    config.Handler,
		db:         db,
		repository: config.Repository,
		schedules:  make(map[int]*Schedule),
		reloadSchedule: Schedule{
			Id:       math.MinInt,
			Name:     "<reload>",
			IsActive: true,
			Cron:     &config.ReloadCron,
		},
	}
	return &scheduler
}

func (s *Scheduler) Start() {
	s.StartContext(context.Background())
}

func (s *Scheduler) StartContext(ctx context.Context) {
	if s.running.Load() {
		panic(errors.New("already running"))
	}

	s.stoped.Add(1)
	ctx, s.cancel = context.WithCancel(ctx)
	go s.run(ctx)
}

func (s *Scheduler) Stop() {
	s.log.Debug("Stopping")
	s.cancel()
	s.stoped.Wait()
	s.log.Debug("Stopped")
}

func (s *Scheduler) run(ctx context.Context) {
	s.log.Debug("scheduler is stated")
	defer func() {
		s.log.Debug("scheduler is stopped")
	}()

	s.running.Store(true)
	defer func() {
		s.running.Store(false)
	}()

	defer s.stoped.Done()

	if err := s.reload(); err != nil {
		s.log.Error("cannot load schedules", "error", err)
		panic(err)
	}

	for {
		sched := s.getNext()
		d := time.Until(*sched.NextRunAt)
		s.log.Debug("next fire time", "time", *sched.NextRunAt, "duration", d)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			s.log.Debug("terminate")
			clear(s.schedules)
			return
		case <-timer.C:
			if sched == &s.reloadSchedule {
				if err := s.reload(); err != nil {
					s.log.Error("cannot load schedules", "error", err)
					panic(err)
				}
			} else {
				if err := s.processTask(sched); err != nil {
					s.log.Error("cannot process task", "schedule", sched, "error", err)
				}
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Debug("reload schdules")

	if s.reloadSchedule.NextRunAt == nil {
		if nextTs, err := gronx.NextTick(*s.reloadSchedule.Cron, false); err != nil {
			return err
		} else {
			s.reloadSchedule.NextRunAt = &nextTs
		}
	}

	loaded := make(map[int]*Schedule)
	err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		schedules, err := s.repository.FindAllActive(tx)
		if err != nil {
			return err
		}
		for _, dbSchedule := range schedules {
			s.log.Info("process schedule", "schedule", dbSchedule)
			if !dbSchedule.IsActive {
				s.log.Debug("the schedule is not active", "schedule", dbSchedule)
				continue
			} else if dbSchedule.NextRunAt == nil && dbSchedule.Cron == nil {
				s.log.Debug("the schedule is not valid")
				dbSchedule.IsActive = false
				if err := s.repository.Save(tx, dbSchedule); err != nil {
					return err
				}
				continue
			} else if dbSchedule.NextRunAt == nil {
				if nextTs, err := gronx.NextTick(*dbSchedule.Cron, true); err != nil {
					s.log.Info("the schedule has an invalid cron expression", "error", err)
					dbSchedule.IsActive = false
					if err := s.repository.Save(tx, dbSchedule); err != nil {
						return err
					}
				} else {
					dbSchedule.NextRunAt = &nextTs
					s.log.Debug("update next fire time", "NextTs", *dbSchedule.NextRunAt)
					if err := s.repository.Save(tx, dbSchedule); err != nil {
						return err
					}
				}
			}
			loaded[dbSchedule.Id] = dbSchedule
		}
		return nil
	})
	if err != nil {
		return err
	}

	current := s.schedules
	for id := range loaded {
		if _, ok := current[id]; ok {
			s.log.Debug("updated schedule", "schedule", id)
		} else {
			s.log.Debug("added schedule", "schedule", id)
		}
	}
	for id := range current {
		if _, ok := loaded[id]; !ok {
			s.log.Debug("deleted schedule", "schedule", id)
		}
	}
	s.schedules = loaded

	return nil
}

func (s *Scheduler) getNext() *Schedule {
	// yes, we should use heap, but I'm lazy
	var next *Schedule = &s.reloadSchedule
	for _, s := range s.schedules {
		if s.NextRunAt.Before(*next.NextRunAt) {
			next = s
		}
	}
	s.log.Debug("next schedule", "schedule", next)
	return next
}

func (s *Scheduler) processTask(schedule *Schedule) error {
	if schedule == nil {
		return errors.New("code bug: schedule is nil")
	}
	s.log.Info("process the schedule", "schedule", schedule)
	return barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		dbSchedule, err := s.repository.FindOne(tx, schedule.Id)
		if err != nil {
			return err
		}
		if dbSchedule == nil {
			s.log.Info("the schedule is not found")
			delete(s.schedules, schedule.Id)
			return nil
		}
		s.log.Info("loaded state from db", "schedule", dbSchedule)
		if dbSchedule.NextRunAt == nil && dbSchedule.Cron == nil {
			s.log.Info("the schedule is not valid", "schedule", schedule.Id)
			dbSchedule.IsActive = false
			if err := s.repository.Save(tx, dbSchedule); err != nil {
				return err
			}
			delete(s.schedules, schedule.Id)
			return nil
		}
		if dbSchedule.NextRunAt.Equal(*schedule.NextRunAt) || dbSchedule.NextRunAt.Before(*schedule.NextRunAt) {
			if err := s.handler(tx, dbSchedule); err != nil {
				s.log.Error("the schedule processed with error", "error", err)
			}
			if dbSchedule.Cron == nil {
				s.log.Info("the schedule is one shot")
				dbSchedule.IsActive = false
				dbSchedule.LastRunAt = dbSchedule.NextRunAt
			} else {
				if nextTs, err := gronx.NextTick(*dbSchedule.Cron, false); err != nil {
					s.log.Info("the schedule has an invalid cron expression", "error", err)
					dbSchedule.IsActive = false
					dbSchedule.LastRunAt = dbSchedule.NextRunAt
				} else {
					s.log.Info("the schedule is planned", "time", nextTs)
					dbSchedule.LastRunAt = dbSchedule.NextRunAt
					dbSchedule.NextRunAt = &nextTs
				}
			}
			if err := s.repository.Save(tx, dbSchedule); err != nil {
				return err
			}
		} else {
			s.log.Info("schedule is changed and will be rescheduled")
		}
		s.schedules[dbSchedule.Id] = dbSchedule
		return nil
	})
}

func dummySchedulerHandler(tx *sql.Tx, s *Schedule) error {
	var payload map[string]interface{}
	if s.Message != nil {
		if err := json.Unmarshal([]byte(*s.Message), &payload); err != nil {
			return err
		}
	} else {
		payload = make(map[string]interface{})
	}
	meta := make(map[string]interface{})
	meta["name"] = s.Name
	meta["moment"] = s.NextRunAt
	payload["_meta"] = meta
	if encodedPayload, err := json.Marshal(payload); err != nil {
		return err
	} else {
		slog.Info("DUMMY: process", "payload", string(encodedPayload))
	}
	return nil
}
