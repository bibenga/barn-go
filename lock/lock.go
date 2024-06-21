package lock

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
)

// Information about lock from DB
type LockState struct {
	Name     string
	LockedAt *time.Time
	Owner    *string
}

// Lock is the mutex entry in the database.
type Lock struct {
	log      *slog.Logger
	name     string
	db       *sql.DB
	query    LockQuery
	lockName string
	hearbeat time.Duration
	ttl      time.Duration
	locked   bool
	lockedAt *time.Time
}

type lockOpt func(locker *Lock)

// The default TTL is 60 sec
const DefaultLockTtl = 60 * time.Second

// The defaulth heartbeat interval is 20 sec
const DefaultHeartbeat = DefaultLockTtl / 3

// NewLock returns a new lock client.
func NewLock(db *sql.DB, name string, lockName string, ttl time.Duration, opts ...lockOpt) *Lock {
	lock := &Lock{
		log:      slog.Default().With("lock", lockName, "name", name),
		name:     name,
		db:       db,
		query:    NewDefaultLockQuery(),
		lockName: lockName,
		hearbeat: ttl / 3,
		ttl:      ttl,
		locked:   false,
		lockedAt: nil,
	}
	for _, opt := range opts {
		opt(lock)
	}
	return lock
}

func WithHostname(name string) lockOpt {
	return func(l *Lock) {
		name, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		if name == "" {
			panic(errors.New("cannot retrieve hostname"))
		}
		l.name = name
	}
}

func WithRandomName() lockOpt {
	return func(l *Lock) {
		uuid, err := uuid.NewRandom()
		if err != nil {
			panic(err)
		}
		l.name = uuid.String()
	}
}

func WithTtl(ttl time.Duration) lockOpt {
	return func(l *Lock) { l.ttl = ttl }
}

func WithHearbeat(hearbeat time.Duration) lockOpt {
	return func(l *Lock) { l.hearbeat = hearbeat }
}

func (l *Lock) Name() string {
	return l.name
}

func (l *Lock) LockName() string {
	return l.lockName
}

func (l *Lock) IsLocked() bool {
	return l.locked
}

func (l *Lock) Ttl() time.Duration {
	return l.ttl
}

func (l *Lock) Hearbeat() time.Duration {
	return l.hearbeat
}

func (l *Lock) LockedAt() *time.Time {
	return l.lockedAt
}

func (l *Lock) CreateTable() error {
	l.log.Info("create lock table")
	_, err := l.db.Exec(l.query.GetCreateTableQuery())
	return err
}

// create a lock record in DB if it doesn't exists
func (l *Lock) Create() (bool, error) {
	l.log.Info("create the lock")

	res, err := l.db.Exec(
		l.query.GetInsertQuery(),
		l.lockName,
	)
	if err != nil {
		l.log.Error("cannot create lock", "error", err)
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		l.log.Error("db error", "error", err)
		return false, err
	}
	l.log.Debug("sql", "RowsAffected", rowsAffected)
	if rowsAffected == 1 {
		l.log.Info("the lock is created")
	} else {
		l.log.Info("the lock was created by someone")
	}
	return rowsAffected == 1, nil
}

// Try to acquire the lock
func (l *Lock) TryLock() (bool, error) {
	if l.locked {
		return false, errors.New("the lock is locked")
	}
	lockedAt := time.Now().UTC()
	rottenTs := lockedAt.Add(-l.ttl)
	res, err := l.db.Exec(
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

// Acquire the lock
func (l *Lock) Lock() (bool, error) {
	return l.LockContext(context.Background())
}

// Acquire the lock
func (l *Lock) LockContext(ctx context.Context) (bool, error) {
	if l.locked {
		return false, errors.New("the lock is locked")
	}
	l.log.Info("lock")
	if locked, err := l.TryLock(); err != nil {
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
			if locked, err := l.TryLock(); err != nil {
				return false, err
			} else {
				return locked, nil
			}
		}
	}
}

// Update the acquired lock data
func (l *Lock) Confirm() (bool, error) {
	if !l.locked {
		return false, errors.New("the lock is not locked")
	}
	lockedAt := time.Now().UTC()
	rottenTs := lockedAt.Add(-l.ttl)
	res, err := l.db.Exec(
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

// Release lock
func (l *Lock) Unlock() (bool, error) {
	if !l.locked {
		return false, errors.New("the lock is not locked")
	}
	rottenTs := time.Now().UTC().Add(-l.ttl)
	res, err := l.db.Exec(
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
}

// Read lock state from DB
func (l *Lock) State() (*LockState, error) {
	stmt, err := l.db.Prepare(l.query.GetSelectQuery())
	if err != nil {
		l.log.Error("cannot prepare query", "error", err)
		return nil, err
	}
	defer stmt.Close()
	var state = LockState{Name: l.lockName}
	row := stmt.QueryRow(l.lockName)
	switch err := row.Scan(&state.LockedAt, &state.Owner); err {
	case nil:
		lockedAtAttr := slog.Any("LockedAt", nil)
		if state.LockedAt != nil {
			lockedAtAttr.Value = slog.TimeValue(*state.LockedAt)
		}
		ownerAttr := slog.Any("Owner", nil)
		if state.Owner != nil {
			ownerAttr.Value = slog.StringValue(*state.Owner)
		}
		l.log.Info("the lock is captured", lockedAtAttr, ownerAttr)
		return &state, nil
	case sql.ErrNoRows:
		l.log.Error("the lock is not found")
		return nil, err
	default:
		l.log.Error("db error", "error", err)
		return nil, err
	}
}
