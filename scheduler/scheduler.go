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

type Task struct {
	Id       int
	Name     string
	IsActive bool
	Cron     *string
	NextTs   *time.Time
	LastTs   *time.Time
	Message  *string
}

func (e Task) LogValue() slog.Value {
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

type TaskMap map[int]*Task
type TaskList []*Task

type SchedulerListener interface {
	Process(name string, moment time.Time, message *string) error
}

type SchedulerConfig struct {
	Log        *slog.Logger
	Query      *TaskQuery
	ReloadCron string
	Listener   SchedulerListener
}

type Scheduler struct {
	log        *slog.Logger
	listener   SchedulerListener
	db         *sql.DB
	query      *TaskQuery
	tasks      TaskMap
	reloadTask Task
	running    atomic.Bool
	cancel     context.CancelFunc
	stoped     sync.WaitGroup
}

func NewScheduler(db *sql.DB, config *SchedulerConfig) *Scheduler {
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
	if config.ReloadCron == "" {
		config.ReloadCron = "*/5 * * * *"
	}
	if config.Listener == nil {
		config.Listener = &DummySchedulerListener{}
	}
	if config.Log == nil {
		config.Log = slog.Default()
	}
	scheduler := Scheduler{
		log:      config.Log,
		listener: config.Listener,
		db:       db,
		query:    config.Query,
		tasks:    make(TaskMap),
		reloadTask: Task{
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
			clear(s.tasks)
			return
		case <-timer.C:
			if e == &s.reloadTask {
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
					if e != &s.reloadTask {
						s.update(e)
					}
				}
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Info("reload")

	if s.reloadTask.NextTs == nil {
		if nextTs, err := gronx.NextTick(*s.reloadTask.Cron, true); err != nil {
			return err
		} else {
			s.reloadTask.NextTs = &nextTs
		}
	}

	tasks, err := s.getTasks()
	if err != nil {
		return err
	}

	for id, newTask := range tasks {
		if oldTask, ok := s.tasks[id]; ok {
			// exists
			if s.IsChanged(oldTask, newTask) {
				// changed
				s.log.Info("changed task", "task", newTask)
				// s.entries[id] = newEntry
				s.tasks[id] = newTask
			} else {
				s.log.Info("unchanged task", "task", newTask)
				oldTask.Name = newTask.Name
				oldTask.Message = newTask.Message
			}
		} else {
			// added
			s.log.Info("new task", "task", newTask.Id)
			s.tasks[id] = newTask
		}
		if newTask.NextTs == nil {
			if nextTs, err := gronx.NextTick(*newTask.Cron, true); err != nil {
				return err
			} else {
				newTask.NextTs = &nextTs
				s.update(newTask)
			}
		}
	}

	for id, oldTask := range s.tasks {
		if _, ok := tasks[id]; !ok {
			s.log.Info("deleted task", "task", oldTask.Id)
			delete(s.tasks, oldTask.Id)
		}
	}

	return nil
}

func (s *Scheduler) IsChanged(oldEntry *Task, newEntry *Task) bool {
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

func (s *Scheduler) getNext() *Task {
	// yes, we should use heap, but I'm lazy
	var next *Task = &s.reloadTask
	for _, task := range s.tasks {
		if task.NextTs.Before(*next.NextTs) {
			next = task
		}
	}
	return next
}

func (s *Scheduler) processTask(task *Task) error {
	if task != nil {
		// process
		s.log.Info("tik ", "task", task.Id, "nextTs", task.NextTs)
		s.listener.Process(task.Name, *task.NextTs, task.Message)
		// calculate next time
		if task.Cron != nil {
			nextTs, err := gronx.NextTick(*task.Cron, false)
			if err != nil {
				s.log.Info("cron is invalid", "task", task)
				task.IsActive = false
			} else {
				task.LastTs = task.NextTs
				task.NextTs = &nextTs
			}
		} else {
			task.IsActive = false
		}
		err := s.update(task)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) getTasks() (TaskMap, error) {
	stmt, err := s.db.Prepare(s.query.SelectQuery)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	entries := make(TaskMap)

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var t Task = Task{}
		err := rows.Scan(&t.Id, &t.Name, &t.IsActive, &t.Cron, &t.NextTs, &t.LastTs, &t.Message)
		if err != nil {
			return nil, err
		}
		if t.IsActive {
			if t.Cron == nil && t.NextTs == nil {
				// we don't know when to do...
				s.log.Warn("invalid task", "task", t)
				s.deactivate(&t)
			} else {
				if t.NextTs == nil {
					nextTs, err := gronx.NextTick(*t.Cron, true)
					if err != nil {
						s.log.Info("invalid cron string", "task", t)
						continue
					}
					t.NextTs = &nextTs
					s.update(&t)
				}
				s.log.Info("the task is active", "task", t)
				entries[t.Id] = &t
			}
		} else {
			s.log.Info("the task is inactive", "task", t)
		}
	}
	return entries, nil
}

func (s *Scheduler) update(task *Task) error {
	s.log.Info("update the task", "task", task)
	res, err := s.db.Exec(
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
		// not an erros, need to reload entries...
		return fmt.Errorf("an object deleted")
	}
	return nil
}

func (s *Scheduler) deactivate(task *Task) error {
	task.IsActive = false
	s.log.Info("deactivate the task", "task", task)
	res, err := s.db.Exec(
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
		s.log.Info("the task was deleted somewhen", "task", task)
	}
	return nil
}

func (s *Scheduler) Add(task *Task) error {
	if task.Cron == nil && task.NextTs == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}

	stmt, err := s.db.Prepare(s.query.InsertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(task.Name, task.Cron, task.NextTs, task.Message).Scan(&task.Id)
	if err != nil {
		return err
	}
	s.log.Info("the task is created", "task", task)
	return nil
}

func (s *Scheduler) Delete(id int) error {
	s.log.Info("delete the task", "task", id)
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
		s.log.Info("the task was already deleted", "task", id)
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

type DummySchedulerListener struct {
}

func (l *DummySchedulerListener) Process(name string, moment time.Time, message *string) error {
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

var _ SchedulerListener = &DummySchedulerListener{}
