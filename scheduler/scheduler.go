package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/adhocore/gronx"
)

type Entry struct {
	Id       int
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
	return slog.GroupValue(args...)
}

type EntryMap map[int]*Entry

type SchedulerListener interface {
	Process(name string, moment time.Time, message *string) error
}

type SchedulerConfig struct {
	Log        *slog.Logger
	Query      *EntryQuery
	ReloadCron string
	Listener   SchedulerListener
}

type Scheduler struct {
	log         *slog.Logger
	listener    SchedulerListener
	db          *sql.DB
	query       *EntryQuery
	entries     EntryMap
	reloadEntry Entry
}

func NewScheduler(db *sql.DB, config *SchedulerConfig) *Scheduler {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Query == nil {
		query := NewDefaultEntryQuery()
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
		entries:  make(EntryMap),
		reloadEntry: Entry{
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
	go s.Run(ctx)
}

func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("started")

	if err := s.reload(); err != nil {
		s.log.Error("db", "error", err)
		panic(err)
	}

	for {
		e := s.getNext()
		d := time.Until(*e.NextTs)
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			s.log.Info("terminate")
			return
		case <-timer.C:
			if e == &s.reloadEntry {
				if err := s.reload(); err != nil {
					s.log.Error("db", "error", err)
					panic(err)
				}
			} else {
				if err := s.processEntry(e); err != nil {
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
					if e != &s.reloadEntry {
						s.update(e)
					}
				}
			}
		}
	}
}

func (s *Scheduler) reload() error {
	s.log.Info("reload")

	if s.reloadEntry.NextTs == nil {
		if nextTs, err := gronx.NextTick(*s.reloadEntry.Cron, true); err != nil {
			return err
		} else {
			s.reloadEntry.NextTs = &nextTs
		}
	}

	entries, err := s.getEntries()
	if err != nil {
		return err
	}

	for id, newEntry := range entries {
		if oldEntry, ok := s.entries[id]; ok {
			// exists
			if s.IsChanged(oldEntry, newEntry) {
				// changed
				s.log.Info("changed entry", "entry", newEntry)
				// s.entries[id] = newEntry
				s.entries[id] = newEntry
			} else {
				s.log.Info("unchanged entry", "entry", newEntry)
				oldEntry.Name = newEntry.Name
				oldEntry.Message = newEntry.Message
			}
		} else {
			// added
			s.log.Info("new entry", "entry", newEntry.Id)
			s.entries[id] = newEntry
		}
		if newEntry.NextTs == nil {
			if nextTs, err := gronx.NextTick(*newEntry.Cron, true); err != nil {
				return err
			} else {
				newEntry.NextTs = &nextTs
				s.update(newEntry)
			}
		}
	}

	for id, oldEntry := range s.entries {
		if _, ok := entries[id]; !ok {
			s.log.Info("deleted entry", "entry", oldEntry.Id)
			delete(s.entries, oldEntry.Id)
		}
	}

	return nil
}

func (s *Scheduler) IsChanged(oldEntry *Entry, newEntry *Entry) bool {
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

func (s *Scheduler) getNext() *Entry {
	// yes, we should use heap, but I'm lazy
	var next *Entry = &s.reloadEntry
	for _, entry := range s.entries {
		if entry.NextTs.Before(*next.NextTs) {
			next = entry
		}
	}
	return next
}

func (s *Scheduler) processEntry(entry *Entry) error {
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
	stmt, err := s.db.Prepare(s.query.SelectQuery)
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

func (s *Scheduler) update(entry *Entry) error {
	s.log.Info("update the entry", "entry", entry)
	res, err := s.db.Exec(
		s.query.UpdateQuery,
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
		s.query.UpdateIsActiveQuery,
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

func (s *Scheduler) Add(entry *Entry) error {
	if entry.Cron == nil && entry.NextTs == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}

	stmt, err := s.db.Prepare(s.query.InsertQuery)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(entry.Name, entry.Cron, entry.NextTs, entry.Message).Scan(&entry.Id)
	if err != nil {
		return err
	}
	s.log.Info("the entry is created", "entry", entry)
	return nil
}

func (s *Scheduler) Delete(id int) error {
	s.log.Info("delete the entry", "id", id)
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
		s.log.Info("the entry was already deleted", "entry", id)
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
