package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adhocore/gronx"
)

type Schedule struct {
	Id       int
	Name     string
	IsActive bool
	Cron     *string
	NextTs   *time.Time
	LastTs   *time.Time
	Message  *string
}

func (e Schedule) LogValue() slog.Value {
	// return slog.AnyValue(computeExpensiveValue(e.arg))
	var args []slog.Attr
	args = append(args, slog.Int("Id", int(e.Id)))
	args = append(args, slog.Bool("IsActive", e.IsActive))
	if e.Cron != nil {
		args = append(args, slog.String("Cron", *e.Cron))
	}
	if e.NextTs != nil {
		args = append(args, slog.String("NextTs", e.NextTs.String()))
	}
	return slog.GroupValue(args...)
}

type ScheduleMap map[int]*Schedule
type ScheduleList []*Schedule

type SchedulerHandler func(db *sql.DB, name string, moment time.Time, message *string) error

type SchedulerConfig struct {
	Log        *slog.Logger
	Query      *ScheduleQuery
	ReloadCron string
	Handler    SchedulerHandler
}

type Scheduler struct {
	log            *slog.Logger
	handler        SchedulerHandler
	db             *sql.DB
	query          *ScheduleQuery
	schedules      ScheduleMap
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
	if config.Query == nil {
		config.Query = NewDefaultScheduleQuery()
	}
	if config.ReloadCron == "" {
		config.ReloadCron = "*/5 * * * *"
	}
	if config.Handler == nil {
		config.Handler = dummySchedulerHandler
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := Scheduler{
		log:       config.Log,
		handler:   config.Handler,
		db:        db,
		query:     config.Query,
		schedules: make(ScheduleMap),
		reloadSchedule: Schedule{
			Id:       math.MinInt,
			Name:     "<reload>",
			IsActive: true,
			Cron:     &config.ReloadCron,
		},
	}
	return &scheduler
}

func (s *Scheduler) CreateTable() error {
	s.log.Info("create table")
	_, err := s.db.Exec(s.query.CreateTableQuery)
	return err
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
	s.log.Debug("scheduler stated")
	defer func() {
		s.log.Debug("scheduler terminated")
	}()

	s.running.Store(true)
	defer func() {
		s.running.Store(false)
	}()

	defer s.stoped.Done()

	if err := s.reload(); err != nil {
		s.log.Error("db", "error", err)
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
			if e.Cron == nil {
				s.deactivate(e)
			} else {
				if nextTs, err := gronx.NextTickAfter(*e.Cron, *e.NextTs, false); err != nil {
					panic(err)
				} else {
					e.NextTs = &nextTs
					if e != &s.reloadSchedule {
						s.update(e)
					}
				}
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Info("reload")

	if s.reloadSchedule.NextTs == nil {
		if nextTs, err := gronx.NextTick(*s.reloadSchedule.Cron, true); err != nil {
			return err
		} else {
			s.reloadSchedule.NextTs = &nextTs
		}
	}

	schedules, err := s.getSchedules()
	if err != nil {
		return err
	}

	for id, newSchedule := range schedules {
		if oldSchedule, ok := s.schedules[id]; ok {
			// exists
			if s.IsChanged(oldSchedule, newSchedule) {
				// changed
				s.log.Info("changed schedule", "schedule", newSchedule)
				// s.entries[id] = newEntry
				s.schedules[id] = newSchedule
			} else {
				s.log.Info("unchanged taschedulesk", "schedule", newSchedule)
				oldSchedule.Name = newSchedule.Name
				oldSchedule.Message = newSchedule.Message
			}
		} else {
			// added
			s.log.Info("new schedule", "schedule", newSchedule.Id)
			s.schedules[id] = newSchedule
		}
		if newSchedule.NextTs == nil {
			if nextTs, err := gronx.NextTick(*newSchedule.Cron, true); err != nil {
				return err
			} else {
				newSchedule.NextTs = &nextTs
				s.update(newSchedule)
			}
		}
	}

	for id, oldSchedule := range s.schedules {
		if _, ok := schedules[id]; !ok {
			s.log.Info("deleted schedule", "schedule", oldSchedule.Id)
			delete(s.schedules, oldSchedule.Id)
		}
	}

	return nil
}

func (s *Scheduler) IsChanged(oldEntry *Schedule, newEntry *Schedule) bool {
	if oldEntry.Cron != nil && newEntry.Cron != nil {
		if *oldEntry.Cron != *newEntry.Cron {
			return true
		}
	} else if oldEntry.Cron != nil || newEntry.Cron != nil {
		return true
	}

	if oldEntry.NextTs != nil && newEntry.NextTs != nil {
		if *oldEntry.NextTs != *newEntry.NextTs {
			return true
		}
	} else if oldEntry.NextTs != nil || newEntry.NextTs != nil {
		return true
	}

	return false
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
	if schedule != nil {
		// process
		s.log.Info("tik ", "schedule", schedule.Id, "nextTs", schedule.NextTs)

		if err := s.handler(s.db, schedule.Name, *schedule.NextTs, schedule.Message); err != nil {
			return err
		}

		// calculate next time
		if schedule.Cron != nil {
			nextTs, err := gronx.NextTick(*schedule.Cron, false)
			if err != nil {
				s.log.Info("cron is invalid", "schedule", schedule)
				schedule.IsActive = false
			} else {
				schedule.LastTs = schedule.NextTs
				schedule.NextTs = &nextTs
			}
		} else {
			schedule.IsActive = false
		}
		err := s.update(schedule)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) getSchedules() (ScheduleMap, error) {
	stmt, err := s.db.Prepare(s.query.SelectQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	entries := make(ScheduleMap)

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var schedule Schedule = Schedule{}
		err := rows.Scan(&schedule.Id, &schedule.Name, &schedule.IsActive, &schedule.Cron, &schedule.NextTs, &schedule.LastTs, &schedule.Message)
		if err != nil {
			return nil, err
		}
		if schedule.IsActive {
			if schedule.Cron == nil && schedule.NextTs == nil {
				// we don't know when to do...
				s.log.Warn("invalid schedule", "schedule", schedule)
				s.deactivate(&schedule)
			} else {
				if schedule.NextTs == nil {
					nextTs, err := gronx.NextTick(*schedule.Cron, true)
					if err != nil {
						s.log.Info("invalid cron string", "schedule", schedule)
						continue
					}
					schedule.NextTs = &nextTs
					s.update(&schedule)
				}
				s.log.Info("the schedule is active", "schedule", schedule)
				entries[schedule.Id] = &schedule
			}
		} else {
			s.log.Info("the schedule is inactive", "schedule", schedule)
		}
	}
	return entries, nil
}

func (s *Scheduler) update(schedule *Schedule) error {
	s.log.Info("update the schedule", "schedule", schedule)
	res, err := s.db.Exec(
		s.query.UpdateQuery,
		schedule.IsActive, schedule.NextTs, schedule.LastTs,
		schedule.Id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		// not an erros, need to reload entries...
		return fmt.Errorf("an object deleted")
	}
	return nil
}

func (s *Scheduler) deactivate(schedule *Schedule) error {
	schedule.IsActive = false
	s.log.Info("deactivate the schedule", "schedule", schedule)
	res, err := s.db.Exec(
		s.query.UpdateIsActiveQuery,
		false, schedule.Id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		s.log.Info("the schedule was deleted somewhen", "schedule", schedule)
	}
	return nil
}

func (s *Scheduler) Add(schedule *Schedule) error {
	if schedule.Cron == nil && schedule.NextTs == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}

	stmt, err := s.db.Prepare(s.query.InsertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(schedule.Name, schedule.Cron, schedule.NextTs, schedule.Message).Scan(&schedule.Id)
	if err != nil {
		return err
	}
	s.log.Info("the schedule is created", "schedule", schedule)
	return nil
}

func (s *Scheduler) Delete(id int) error {
	s.log.Info("delete the schedule", "schedule", id)
	res, err := s.db.Exec(
		s.query.DeleteQuery,
		id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		s.log.Info("the schedule was already deleted", "schedule", id)
	}
	return nil
}

func (s *Scheduler) DeleteAll() error {
	s.log.Info("delete all entries")
	res, err := s.db.Exec(s.query.DeleteAllQuery)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	s.log.Info("all entries is deleted", "RowsAffected", rowsAffected)
	return nil
}

func dummySchedulerHandler(db *sql.DB, name string, moment time.Time, message *string) error {
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
