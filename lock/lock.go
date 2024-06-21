package lock

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
)

type Lock struct {
	log      *slog.Logger
	name     string
	db       *sql.DB
	query    LockQuery
	listener LockListener
	lockName string
	hearbeat time.Duration
	ttl      time.Duration
	locked   bool
	lockedAt *time.Time
	stop     chan struct{}
	stopped  chan struct{}
}

type LockListener interface {
	OnLock()
	OnUnlock()
}

func NewLock(db *sql.DB, lockName string, hearbeat time.Duration, ttl time.Duration, listener LockListener) *Lock {
	name, err := os.Hostname()
	if err != nil {
		slog.Warn("can't get hostname", "error", err)
		uuid, err := uuid.NewRandom()
		if err != nil {
			panic(err)
		}
		name = uuid.String()
	}

	lock := Lock{
		log:      slog.Default().With("lock", lockName, "name", name),
		name:     name,
		db:       db,
		query:    NewDefaultLockQuery(),
		listener: listener,
		lockName: lockName,
		hearbeat: 1 * time.Second,
		ttl:      10 * time.Second,
		locked:   false,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
	return &lock
}

func (l *Lock) CreateTable() error {
	db := l.db
	l.log.Warn("create lock table")
	_, err := db.Exec(l.query.GetCreateTableQuery())
	return err
}

func (l *Lock) Start() error {
	if err := l.create(); err != nil {
		return err
	}
	go l.Run()
	return nil
}

func (l *Lock) Stop() {
	l.log.Info("stopping")
	l.stop <- struct{}{}
	<-l.stopped
	close(l.stop)
	close(l.stopped)
	l.log.Info("stopped")
}

func (l *Lock) Run() {
	if err := l.create(); err != nil {
		panic(err)
	}

	if err := l.check(); err != nil {
		panic(err)
	}

	check := time.NewTicker(l.hearbeat)
	defer check.Stop()

	l.log.Info("started")
	for {
		select {
		case <-l.stop:
			l.log.Info("terminate")
			l.stopped <- struct{}{}
			if l.locked {
				if released, err := l.unlock(); err != nil {
					panic(err)
				} else {
					if released {
						l.onUnlock()
					}
				}
			}
			return
		case <-check.C:
			if err := l.check(); err != nil {
				panic(err)
			}
		}
	}
}

func (l *Lock) check() error {
	if l.locked {
		if confirmed, err := l.confirm(); err != nil {
			return err
		} else {
			if confirmed {
				l.log.Info("the lock is still owned me")
			} else {
				l.log.Warn("the lock has been acquired by someone unexpectedly")
				l.onUnlock()
			}
		}
	} else {
		if acquired, err := l.tryLock(); err != nil {
			return err
		} else {
			if acquired {
				l.log.Info("the lock is rotten and acquired")
				l.onLock()
			} else {
				return l.logState()
			}
		}
	}
	return nil
}

// create lock in the table if it doesn't exists
func (l *Lock) create() error {
	db := l.db
	l.log.Info("create the lock")

	res, err := db.Exec(
		l.query.GetInsertQuery(),
		l.lockName,
	)
	if err != nil {
		l.log.Error("cannot create lock", "error", err)
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		l.log.Error("db error", "error", err)
		return err
	}
	l.log.Debug("sql", "RowsAffected", rowsAffected)
	if rowsAffected == 1 {
		l.log.Info("the lock is created")
	} else {
		l.log.Info("the lock was created by someone")
	}
	return nil
}

func (l *Lock) tryLock() (bool, error) {
	db := l.db
	lockedAt := time.Now().UTC()
	rottenTs := lockedAt.Add(-l.ttl)
	res, err := db.Exec(
		l.query.GetLockQuery(),
		l.name, lockedAt,
		l.lockName, rottenTs,
	)
	if err != nil {
		l.log.Error("db error", "error", err)
		return false, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		l.log.Error("db error", "error", err)
		return false, err
	} else {
		l.log.Debug("sql", "RowsAffected", rowsAffected)
		if rowsAffected == 1 {
			l.locked = true
			l.lockedAt = &lockedAt
		}
		return l.locked, nil
	}
}

func (l *Lock) lock(ctx context.Context) (bool, error) {
	l.log.Info("lock")
	if locked, err := l.tryLock(); err != nil {
		return false, err
	} else {
		if locked {
			return locked, nil
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, nil
		case <-ticker.C:
			if locked, err := l.tryLock(); err != nil {
				return false, err
			} else {
				return locked, nil
			}
		}
	}
}

func (l *Lock) confirm() (bool, error) {
	db := l.db
	lockedAt := time.Now().UTC()
	rottenTs := time.Now().UTC().Add(-l.ttl)
	res, err := db.Exec(
		l.query.GetConfirmQuery(),
		l.name, lockedAt,
		l.lockName, l.name, rottenTs,
	)
	if err != nil {
		l.log.Error("db error", "error", err)
		return false, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		l.log.Error("db error", "error", err)
		return false, err
	} else {
		l.log.Debug("sql", "RowsAffected", rowsAffected)
		if rowsAffected == 1 {
			l.lockedAt = &lockedAt
		} else {
			l.locked = false
			l.lockedAt = nil
		}
		return l.locked, nil
	}
}

func (l *Lock) unlock() (bool, error) {
	if l.locked {
		db := l.db
		rottenTs := time.Now().UTC().Add(-l.ttl)
		res, err := db.Exec(
			l.query.GetUnlockQuery(),
			l.lockName, l.name, rottenTs,
		)
		if err != nil {
			l.log.Error("db error", "error", err)
			return false, err
		}
		if rowsAffected, err := res.RowsAffected(); err != nil {
			l.log.Error("db error", "error", err)
			return false, err
		} else {
			l.log.Debug("sql", "RowsAffected", rowsAffected)
			l.locked = false
			l.lockedAt = nil
			if rowsAffected == 1 {
				l.log.Info("the lock is released")
			} else {
				l.log.Info("the lock was created by someone")
			}
			return rowsAffected == 1, nil
		}
	} else {
		l.log.Info("the lock is not our")
	}
	return false, nil
}

func (l *Lock) logState() error {
	db := l.db
	stmt, err := db.Prepare(l.query.GetSelectQuery())
	if err != nil {
		l.log.Error("cannot prepare query", "error", err)
		return err
	}
	defer stmt.Close()
	var lockedAt *time.Time
	var owner *string
	row := stmt.QueryRow(l.lockName)
	switch err := row.Scan(&lockedAt, &owner); err {
	case sql.ErrNoRows:
		l.log.Error("the lock is not found")
		return nil
	case nil:
		lockedAtAttr := slog.Any("LockedAt", nil)
		if lockedAt != nil {
			lockedAtAttr.Value = slog.TimeValue(*lockedAt)
		}
		ownerAttr := slog.Any("Owner", nil)
		if owner != nil {
			ownerAttr.Value = slog.StringValue(*owner)
		}
		l.log.Info("the lock is captured", lockedAtAttr, ownerAttr)
		return nil
	default:
		l.log.Error("db error", "error", err)
		return err
	}
}

func (l *Lock) onLock() {
	if l.listener != nil {
		l.listener.OnLock()
	}
}

func (l *Lock) onUnlock() {
	if l.listener != nil {
		l.listener.OnUnlock()
	}
}

type DummyLockListener struct{}

func (l *DummyLockListener) OnLock() {
	slog.Info("DUMMY: the lock is acquired")
}

func (l *DummyLockListener) OnUnlock() {
	slog.Info("DUMMY: the lock is released")
}

var _ LockListener = &DummyLockListener{}
