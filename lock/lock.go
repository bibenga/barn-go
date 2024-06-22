package lock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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

type LockConfig struct {
	Log      *slog.Logger
	Name     string
	Query    *LockQuery
	LockName string
	Ttl      time.Duration
	Hearbeat time.Duration
}

// Lock is the mutex entry in the database.
type Lock struct {
	log      *slog.Logger
	name     string
	db       *sql.DB
	query    *LockQuery
	lockName string
	ttl      time.Duration
	hearbeat time.Duration
	locked   bool
	lockedAt *time.Time
}

// The default TTL is 60 sec
const DefaultLockTtl = 60 * time.Second

// The defaulth heartbeat interval is 20 sec
const DefaultHeartbeat = DefaultLockTtl / 3

func NewLock(db *sql.DB) *Lock {
	return NewLockWithConfig(db, &LockConfig{})
}

// NewLockWithConfig create new Lock with config, during initialization config is adjusted
func NewLockWithConfig(db *sql.DB, config *LockConfig) *Lock {
	if db == nil {
		panic(errors.New("db is nil"))
	}
	if config == nil {
		panic(errors.New("config is nil"))
	}
	if config.Name == "" {
		name, err := os.Hostname()
		if err != nil {
			config.Log.Warn("cannot retrieve hostname", "error", err)
		}
		if name == "" {
			uuid, err := uuid.NewRandom()
			if err != nil {
				panic(err)
			}
			name = uuid.String()
		} else {
			name = fmt.Sprintf("%s-%d", name, os.Getpid())
		}
		config.Name = name
	}
	if config.Query == nil {
		query := NewDefaultLockQuery()
		config.Query = &query
	}
	if config.LockName == "" {
		config.LockName = "barn"
	}
	if config.Ttl == 0 {
		config.Ttl = 60 * time.Second
	}
	if config.Hearbeat == 0 {
		config.Hearbeat = config.Ttl / 3
	}
	if config.Log == nil {
		config.Log = slog.Default().With(
			"lock", slog.GroupValue(
				slog.String("lock", config.LockName),
				slog.String("name", config.Name),
			),
			"lock", slog.GroupValue(
				slog.String("a", config.LockName),
				slog.String("b", config.Name),
			),
		)
	}

	lock := &Lock{
		log:      config.Log,
		name:     config.Name,
		db:       db,
		query:    config.Query,
		lockName: config.LockName,
		ttl:      config.Ttl,
		hearbeat: config.Hearbeat,
		locked:   false,
		lockedAt: nil,
	}
	return lock
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
	_, err := l.db.Exec(l.query.CreateTableQuery)
	return err
}

// create a lock record in DB if it doesn't exists
func (l *Lock) Create() (bool, error) {
	l.log.Info("create the lock")

	res, err := l.db.Exec(
		l.query.InsertQuery,
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
		l.query.LockQuery,
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
		l.query.ConfirmQuery,
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
			l.log.Info("the lock is confirmed")
		} else {
			l.locked = false
			l.lockedAt = nil
			l.log.Warn("the lock is not confirmed")
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
		l.query.UnlockQuery,
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
			l.log.Warn("the lock cannot be released")
		}
		return rowsAffected == 1, nil
	}
}

// Read lock state from DB
func (l *Lock) State() (*LockState, error) {
	stmt, err := l.db.Prepare(l.query.SelectQuery)
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
