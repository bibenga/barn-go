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

type SchedulerHandler func(tx *sql.Tx, name string, moment time.Time, message *string) error

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
		config.Repository = NewDefaultPostgresSchedulerRepository()
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
	if !s.running.Load() {
		panic(errors.New("is not running"))
	}

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
		s.log.Error("cannot load", "error", err)
		panic(err)
	}

	for {
		e := s.getNext()
		s.log.Info("next event time", "NextTs", *e.NextTs)
		d := time.Until(*e.NextTs)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			s.log.Info("terminate")
			clear(s.schedules)
			return
		case <-timer.C:
			if e == &s.reloadSchedule {
				if err := s.reload(); err != nil {
					s.log.Error("db", "error", err)
					panic(err)
				}
			} else {
				if err := s.processTask(e); err != nil {
					s.log.Error("db", "error", err)
				}
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Info("reload")

	if s.reloadSchedule.NextTs == nil {
		if nextTs, err := gronx.NextTick(*s.reloadSchedule.Cron, false); err != nil {
			return err
		} else {
			s.reloadSchedule.NextTs = &nextTs
		}
	}

	loaded := make(map[int]*Schedule)
	err := barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		schedules, err := s.repository.FindAllActive(tx)
		if err != nil {
			return err
		}
		for _, newSchedule := range schedules {
			s.log.Debug("loaded schedule", "schedule", newSchedule)

			if !newSchedule.IsActive {
				s.log.Debug("loaded schedule", "schedule", newSchedule)
				continue
			}

			if newSchedule.NextTs == nil && newSchedule.Cron == nil {
				newSchedule.IsActive = false
				if err := s.repository.Save(tx, newSchedule); err != nil {
					return err
				}
				continue
			} else if newSchedule.NextTs == nil {
				if nextTs, err := gronx.NextTick(*newSchedule.Cron, true); err != nil {
					s.log.Info("invalid cron expression", "schedule", newSchedule)
					newSchedule.IsActive = false
					if err := s.repository.Save(tx, newSchedule); err != nil {
						return err
					}
				} else {
					newSchedule.NextTs = &nextTs
					if err := s.repository.Save(tx, newSchedule); err != nil {
						return err
					}
				}
			}
			loaded[newSchedule.Id] = newSchedule
		}
		return nil
	})
	if err != nil {
		return err
	}

	current := s.schedules
	for id := range loaded {
		if _, ok := current[id]; ok {
			s.log.Info("updated schedule", "schedule", id)
		} else {
			s.log.Info("added schedule", "schedule", id)
		}
	}
	for id := range current {
		if _, ok := loaded[id]; !ok {
			s.log.Info("deleted schedule", "schedule", id)
		}
	}
	s.schedules = loaded

	return nil
}

func (s *Scheduler) getNext() *Schedule {
	// yes, we should use heap, but I'm lazy
	var next *Schedule = &s.reloadSchedule
	for _, s := range s.schedules {
		if s.NextTs.Before(*next.NextTs) {
			next = s
		}
	}
	return next
}

func (s *Scheduler) processTask(schedule *Schedule) error {
	if schedule == nil {
		return errors.New("code bug: schedule is nil")
	}
	return barngo.RunInTransaction(s.db, func(tx *sql.Tx) error {
		dbSchedule, err := s.repository.FindOne(tx, schedule.Id)
		if err != nil {
			return err
		}
		if dbSchedule == nil {
			s.log.Info("schedule is not active or was deleted", "schedule", schedule.Id)
			delete(s.schedules, schedule.Id)
			return nil
		}
		if dbSchedule.NextTs == nil && dbSchedule.Cron == nil {
			s.log.Info("schedule is not valid", "schedule", schedule.Id)
			dbSchedule.IsActive = false
			if err := s.repository.Save(tx, dbSchedule); err != nil {
				return err
			}
		}
		if dbSchedule.NextTs.Equal(*schedule.NextTs) || dbSchedule.NextTs.Before(*schedule.NextTs) {
			if err := s.handler(tx, schedule.Name, *schedule.NextTs, schedule.Message); err != nil {
				return err
			}
			if dbSchedule.Cron == nil {
				dbSchedule.IsActive = false
				dbSchedule.LastTs = dbSchedule.NextTs
			} else {
				if nextTs, err := gronx.NextTick(*dbSchedule.Cron, false); err != nil {
					s.log.Info("invalid cron expression", "schedule", dbSchedule.Id, "error", err)
					dbSchedule.IsActive = false
					dbSchedule.LastTs = dbSchedule.NextTs
				} else {
					dbSchedule.LastTs = dbSchedule.NextTs
					dbSchedule.NextTs = &nextTs
				}
			}
			if err := s.repository.Save(tx, dbSchedule); err != nil {
				return err
			}
		} else {
			s.log.Info("schedule is changed and will be rescheduled", "schedule", schedule.Id)
		}
		s.schedules[dbSchedule.Id] = dbSchedule
		return nil
	})
}

func dummySchedulerHandler(tx *sql.Tx, name string, moment time.Time, message *string) error {
	var payload map[string]interface{}
	if message != nil {
		if err := json.Unmarshal([]byte(*message), &payload); err != nil {
			return err
		}
	} else {
		payload = make(map[string]interface{})
	}
	meta := make(map[string]interface{})
	meta["name"] = name
	meta["moment"] = moment
	payload["_meta"] = meta
	if encodedPayload, err := json.Marshal(payload); err != nil {
		return err
	} else {
		slog.Info("DUMMY: process", "payload", string(encodedPayload))
	}
	return nil
}
