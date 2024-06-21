package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/adhocore/gronx"
)

type Entry struct {
	Id       int32
	Name     string
	IsActive bool
	Cron     *string
	NextTs   *time.Time
	LastTs   *time.Time
	Message  *string
}

func (e Entry) LogValue() slog.Value {
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
	return slog.GroupValue(
		// slog.Int("Id", int(e.Id)),
		// // slog.String("Name", e.Name),
		// slog.Bool("IsActive", e.IsActive),
		// // slog.Any("Cron", e.Cron),
		// // slog.Any("NextTs", e.NextTs),
		// // slog.Any("Message", e.Message),
		args...,
	)
}

func (e *Entry) IsChanged(o *Entry) bool {
	if e.Cron != o.Cron {
		slog.Info("1 - changed entry", "Cron1", e.Cron, "Cron2", o.Cron)
		return true
	}

	if e.NextTs != o.NextTs {
		slog.Info("3 - changed entry", "NextTs1", e.NextTs, "NextTs2", o.NextTs)
		return true
	}

	return false
}

type EntryMap map[int32]*Entry

type SchedulerListener interface {
	Process(name string, moment time.Time, message *string) error
}

type Scheduler struct {
	log      *slog.Logger
	listener SchedulerListener
	entries  EntryMap
	db       *sql.DB
	query    EntryQuery
	stop     chan struct{}
	stopped  chan struct{}
	timer    *time.Timer
	entry    *Entry
}

func NewScheduler(db *sql.DB, listener SchedulerListener) *Scheduler {
	scheduler := Scheduler{
		log:      slog.Default(),
		listener: listener,
		entries:  make(EntryMap),
		db:       db,
		query:    NewDefaultEntryQuery(),
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	return &scheduler
}

func (s *Scheduler) CreateTable() error {
	s.log.Info("create table")
	_, err := s.db.Exec(s.query.GetCreateTableQuery())
	return err
}

func (s *Scheduler) Stop() {
	s.log.Info("stopping")
	s.stop <- struct{}{}
	<-s.stopped
	close(s.stop)
	close(s.stopped)
	s.log.Info("stopped")
}

func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("started")

	err := s.reload()
	if err != nil {
		s.log.Error("db", "error", err)
		panic(err)
	}

	reloader := time.NewTicker(5 * time.Second)
	defer reloader.Stop()

	// I don't know how to be
	s.timer = time.NewTimer(1 * time.Second)
	defer s.timer.Stop()
	select {
	case <-s.timer.C:
	default:
	}
	s.scheduleNext()

	for {
		select {
		case <-ctx.Done():
			s.log.Info("terminate")
			return
		case <-s.stop:
			s.log.Info("terminate")
			s.stopped <- struct{}{}
			return
		case <-s.timer.C:
			err = s.processEntry()
			if err != nil {
				s.log.Error("db", "error", err)
				// panic(err)
			}
			s.scheduleNext()
		case <-reloader.C:
			err = s.reload()
			if err != nil {
				s.log.Error("db", "error", err)
				// panic(err)
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Info("reload")

	entries, err := s.getEntries()
	if err != nil {
		return err
	}

	for id, newEntry := range entries {
		if oldEntry, ok := s.entries[id]; ok {
			// exists
			if oldEntry.IsChanged(newEntry) {
				// changed
				s.log.Info("changed entry", "entry", newEntry)
				if newEntry.NextTs == nil {
					nextTs, err := gronx.NextTick(*newEntry.Cron, true)
					if err != nil {
						return err
					}
					newEntry.NextTs = &nextTs
					s.update(newEntry)
				}
				s.entries[id] = newEntry

				s.entry = nil
			} else {
				oldEntry.Name = newEntry.Name
				oldEntry.Message = newEntry.Message
			}
		} else {
			// added
			s.log.Info("new entry", "entry", newEntry.Id)
			s.entries[id] = newEntry
		}
	}

	for id, oldEntry := range s.entries {
		if _, ok := entries[id]; !ok {
			s.log.Info("deleted entry", "entry", oldEntry.Id)
			delete(s.entries, oldEntry.Id)
		}
	}

	// scheduler.entries = entries

	if s.entry != nil {
		entry2 := s.entries[s.entry.Id]
		// slog.Info("RESCHEDULE", "entry", scheduler.entry, "entry2", entry2)
		if entry2 != s.entry {
			// object changed
			s.log.Info("RESCHEDULE", "entry", s.entry)
			s.scheduleNext()
		}
	}
	return nil
}

func (s *Scheduler) scheduleNext() {
	var next *Entry = s.getNext()
	// if next != nil && scheduler.entry != nil && next.Id == scheduler.entry.Id {
	// 	return
	// }
	s.entry = next

	var d time.Duration
	if next != nil {
		d = time.Until(*next.NextTs)
		s.log.Info("next", "entry", next.Id, "nextTs", next.NextTs)
	} else {
		d = 1 * time.Second
		s.log.Info("next", "entry", nil)
	}

	// scheduler.timer.Reset(time.Since(*next.NextTs))
	s.timer.Stop()
	select {
	case <-s.timer.C:
	default:
	}
	s.timer.Reset(d)
}

func (s *Scheduler) getNext() *Entry {
	var next *Entry = nil
	for _, entry := range s.entries {
		if next == nil {
			next = entry
			// scheduler.log.Info("=> ", "next", next.NextTs)
		} else {
			// scheduler.log.Info("=> ", "next", next.NextTs, "entry", entry.NextTs)
			if entry.NextTs.Before(*next.NextTs) {
				next = entry
			}
		}
	}
	return next
}

func (s *Scheduler) processEntry() error {
	entry := s.entry
	if entry != nil {
		// process
		s.log.Info("tik ", "entry", entry.Id, "nextTs", entry.NextTs)
		s.listener.Process(entry.Name, *entry.NextTs, entry.Message)
		// calculate next time
		if entry.Cron != nil {
			nextTs, err := gronx.NextTick(*entry.Cron, false)
			if err != nil {
				s.log.Info("cron is invalid", "entry", entry)
				entry.IsActive = false
			} else {
				entry.LastTs = entry.NextTs
				entry.NextTs = &nextTs
			}
		} else {
			entry.IsActive = false
		}
		err := s.update(entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Scheduler) getEntries() (EntryMap, error) {
	stmt, err := s.db.Prepare(s.query.GetSelectAllQuery())
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	entries := make(EntryMap)

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var e Entry = Entry{}
		err := rows.Scan(&e.Id, &e.Name, &e.IsActive, &e.Cron, &e.NextTs, &e.LastTs, &e.Message)
		if err != nil {
			return nil, err
		}
		if e.IsActive {
			if e.Cron == nil && e.NextTs == nil {
				// we don't know when to do...
				s.log.Warn("invalid entry", "entry", e)
				s.deactivate(&e)
			} else {
				if e.NextTs == nil {
					nextTs, err := gronx.NextTick(*e.Cron, true)
					if err != nil {
						s.log.Info("invalid cron string", "entry", e)
						continue
					}
					e.NextTs = &nextTs
					s.update(&e)
				}
				s.log.Info("the entry is active", "entry", e)
				entries[e.Id] = &e
			}
		} else {
			s.log.Info("the entry is inactive", "entry", e)
		}
	}
	return entries, nil
}

func (s *Scheduler) Add(name string, cron *string, nextTs *time.Time, message string) error {
	// fake 1
	// cron := "*/5 * * * * *"
	if cron == nil && nextTs == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}
	if message == "" {
		return fmt.Errorf("invalid message")
	}
	// if cron != nil && nextTs == nil {
	// 	nextTs2, err := gronx.NextTick(*cron, true)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	nextTs = &nextTs2
	// }

	s.log.Info("create the entry", "name", name, "cron", cron, "message", message)
	stmt, err := s.db.Prepare(s.query.GetInsertQuery())
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id int
	err = stmt.QueryRow(name, cron, nextTs, message).Scan(&id)
	if err != nil {
		return err
	}
	s.log.Info("the entry is created", "name", name, "id", id)
	return nil
}

func (s *Scheduler) Delete(id int) error {
	s.log.Info("delete the entry", "id", id)
	res, err := s.db.Exec(
		s.query.GetDeleteQuery(),
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
		s.log.Info("the entry was already deleted", "entry", id)
	}
	return nil
}

func (s *Scheduler) DeleteAll() error {
	s.log.Info("delete all entries")
	res, err := s.db.Exec(s.query.GetDeleteAllQuery())
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

func (s *Scheduler) update(entry *Entry) error {
	s.log.Info("update the entry", "entry", entry)
	res, err := s.db.Exec(
		s.query.GetUpdateQuery(),
		entry.IsActive, entry.NextTs, entry.LastTs, entry.Id,
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

func (s *Scheduler) deactivate(entry *Entry) error {
	entry.IsActive = false
	s.log.Info("deactivate the entry", "entry", entry)
	res, err := s.db.Exec(
		s.query.GetUpdateIsActiveQuery(),
		false, entry.Id,
	)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		s.log.Info("the entry was deleted somewhen", "entry", entry)
	}
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
	meta["name"] = "name"
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
