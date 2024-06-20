package barn

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/adhocore/gronx"
	"github.com/bibenga/barn-go/internal/adapter"
)

type Entry struct {
	Id       int32
	Name     string
	IsActive bool
	Cron     sql.NullString
	NextTs   sql.NullTime
	LastTs   sql.NullTime
	Message  sql.NullString
}

func (e Entry) LogValue() slog.Value {
	// return slog.AnyValue(computeExpensiveValue(e.arg))
	var args []slog.Attr
	args = append(args, slog.Int("Id", int(e.Id)))
	args = append(args, slog.Bool("IsActive", e.IsActive))
	if e.Cron.Valid {
		args = append(args, slog.String("Cron", e.Cron.String))
	}
	if e.NextTs.Valid {
		args = append(args, slog.String("NextTs", e.NextTs.Time.String()))
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
	Process(name string, moment time.Time, message string) error
}

type Scheduler struct {
	log     *slog.Logger
	entries EntryMap
	db      *sql.DB
	query   adapter.EntryQuery
	stop    chan struct{}
	stopped chan struct{}
	timer   *time.Timer
	entry   *Entry
}

func NewScheduler(db *sql.DB) *Scheduler {
	scheduler := Scheduler{
		log:     slog.Default(),
		entries: make(EntryMap),
		db:      db,
		query:   adapter.NewDefaultEntryQuery(),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	return &scheduler
}

func (scheduler *Scheduler) CreateTable() error {
	db := scheduler.db
	scheduler.log.Info("create table")
	_, err := db.Exec(scheduler.query.GetCreateTableQuery())
	return err
}

func (scheduler *Scheduler) Stop() {
	scheduler.log.Info("stopping")
	scheduler.stop <- struct{}{}
	<-scheduler.stopped
	close(scheduler.stop)
	close(scheduler.stopped)
	scheduler.log.Info("stopped")
}

func (scheduler *Scheduler) Run() {
	scheduler.log.Info("started")

	err := scheduler.reload()
	if err != nil {
		scheduler.log.Error("db", "error", err)
		panic(err)
	}

	reloader := time.NewTicker(5 * time.Second)
	defer reloader.Stop()

	// I don't know how to be
	scheduler.timer = time.NewTimer(1 * time.Second)
	defer scheduler.timer.Stop()
	select {
	case <-scheduler.timer.C:
	default:
	}
	scheduler.scheduleNext()

	for {
		select {
		case <-scheduler.stop:
			scheduler.log.Info("terminate")
			scheduler.stopped <- struct{}{}
			return
		case <-scheduler.timer.C:
			err = scheduler.processEntry()
			if err != nil {
				scheduler.log.Error("db", "error", err)
				// panic(err)
			}
			scheduler.scheduleNext()
		case <-reloader.C:
			err = scheduler.reload()
			if err != nil {
				scheduler.log.Error("db", "error", err)
				// panic(err)
			}
		}
	}
}

func (scheduler *Scheduler) reload() error {
	scheduler.log.Info("reload")

	entries, err := scheduler.getEntries()
	if err != nil {
		return err
	}

	for id, newEntry := range entries {
		if oldEntry, ok := scheduler.entries[id]; ok {
			// exists
			if oldEntry.IsChanged(newEntry) {
				// changed
				scheduler.log.Info("changed entry", "entry", newEntry)
				if !newEntry.NextTs.Valid {
					nextTs2, err := gronx.NextTick(newEntry.Cron.String, true)
					if err != nil {
						return err
					}
					newEntry.NextTs = sql.NullTime{Time: nextTs2, Valid: true}
					scheduler.update(newEntry)
				}
				scheduler.entries[id] = newEntry

				scheduler.entry = nil
			} else {
				oldEntry.Name = newEntry.Name
				oldEntry.Message = newEntry.Message
			}
		} else {
			// added
			scheduler.log.Info("new entry", "entry", newEntry.Id)
			scheduler.entries[id] = newEntry
		}
	}

	for id, oldEntry := range scheduler.entries {
		if _, ok := entries[id]; !ok {
			scheduler.log.Info("deleted entry", "entry", oldEntry.Id)
			delete(scheduler.entries, oldEntry.Id)
		}
	}

	// scheduler.entries = entries

	if scheduler.entry != nil {
		entry2 := scheduler.entries[scheduler.entry.Id]
		// slog.Info("RESCHEDULE", "entry", scheduler.entry, "entry2", entry2)
		if entry2 != scheduler.entry {
			// object changed
			scheduler.log.Info("RESCHEDULE", "entry", scheduler.entry)
			scheduler.scheduleNext()
		}
	}
	return nil
}

func (scheduler *Scheduler) scheduleNext() {
	var next *Entry = scheduler.getNext()
	// if next != nil && scheduler.entry != nil && next.Id == scheduler.entry.Id {
	// 	return
	// }
	scheduler.entry = next

	var d time.Duration
	if next != nil {
		d = time.Until(next.NextTs.Time)
		scheduler.log.Info("next", "entry", next.Id, "nextTs", next.NextTs)
	} else {
		d = 1 * time.Second
		scheduler.log.Info("next", "entry", nil)
	}

	// scheduler.timer.Reset(time.Since(*next.NextTs))
	scheduler.timer.Stop()
	select {
	case <-scheduler.timer.C:
	default:
	}
	scheduler.timer.Reset(d)
}

func (scheduler *Scheduler) getNext() *Entry {
	var next *Entry = nil
	for _, entry := range scheduler.entries {
		if next == nil {
			next = entry
			// scheduler.log.Info("=> ", "next", next.NextTs)
		} else {
			// scheduler.log.Info("=> ", "next", next.NextTs, "entry", entry.NextTs)
			if entry.NextTs.Time.Before(next.NextTs.Time) {
				next = entry
			}
		}
	}
	return next
}

func (scheduler *Scheduler) processEntry() error {
	entry := scheduler.entry
	if entry != nil {
		// process
		scheduler.log.Info("tik ", "entry", entry.Id, "nextTs", entry.NextTs)
		// calculate next time
		if entry.Cron.Valid {
			nextTs, err := gronx.NextTick(entry.Cron.String, false)
			if err != nil {
				scheduler.log.Info("cron is invalid", "entry", entry)
				entry.IsActive = false
			} else {
				entry.LastTs = entry.NextTs
				entry.NextTs = sql.NullTime{Time: nextTs, Valid: true}
			}
		} else {
			entry.IsActive = false
		}
		err := scheduler.update(entry)
		if err != nil {
			return err
		}
	}
	return nil
}

func (scheduler *Scheduler) getEntries() (EntryMap, error) {
	db := scheduler.db
	stmt, err := db.Prepare(scheduler.query.GetSelectAllQuery())
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
			if !e.Message.Valid {
				// we don't know what to do...
				scheduler.log.Warn("invalid entry", "entry", e)
				scheduler.deactivate(&e)
			} else if !e.Cron.Valid && !e.NextTs.Valid {
				// we don't know when to do...
				scheduler.log.Warn("invalid entry", "entry", e)
				scheduler.deactivate(&e)
			} else {
				if !e.NextTs.Valid {
					nextTs2, err := gronx.NextTick(e.Cron.String, true)
					if err != nil {
						scheduler.log.Info("invalid cron string", "entry", e)
						continue
					}
					e.NextTs = sql.NullTime{Time: nextTs2, Valid: true}
					scheduler.update(&e)
				}
				scheduler.log.Info("the entry is active", "entry", e)
				entries[e.Id] = &e
			}
		} else {
			scheduler.log.Info("the entry is inactive", "entry", e)
		}
	}
	return entries, nil
}

func (scheduler *Scheduler) Add(name string, cron *string, nextTs *time.Time, message string) error {
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

	scheduler.log.Info("create the entry", "name", name, "cron", cron, "message", message)
	db := scheduler.db
	stmt, err := db.Prepare(scheduler.query.GetInsertQuery())
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id int
	err = stmt.QueryRow(name, cron, nextTs, message).Scan(&id)
	if err != nil {
		return err
	}
	scheduler.log.Info("the entry is created", "name", name, "id", id)
	return nil
}

func (scheduler *Scheduler) Delete(id int) error {
	db := scheduler.db

	scheduler.log.Info("delete the entry", "id", id)
	res, err := db.Exec(
		scheduler.query.GetDeleteQuery(),
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
		scheduler.log.Info("the entry was already deleted", "entry", id)
	}
	return nil
}

func (scheduler *Scheduler) DeleteAll() error {
	db := scheduler.db

	scheduler.log.Info("delete all entries")
	res, err := db.Exec(scheduler.query.GetDeleteAllQuery())
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	scheduler.log.Info("all entries is deleted", "RowsAffected", rowsAffected)
	return nil
}

func (scheduler *Scheduler) update(entry *Entry) error {
	db := scheduler.db

	scheduler.log.Info("update the entry", "entry", entry)
	res, err := db.Exec(
		scheduler.query.GetUpdateQuery(),
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

func (scheduler *Scheduler) deactivate(entry *Entry) error {
	db := scheduler.db

	entry.IsActive = false
	scheduler.log.Info("deactivate the entry", "entry", entry)
	res, err := db.Exec(
		scheduler.query.GetUpdateIsActiveQuery(),
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
		scheduler.log.Info("the entry was deleted somewhen", "entry", entry)
	}
	return nil
}
