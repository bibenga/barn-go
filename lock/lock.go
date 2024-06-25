package lock

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	barngo "github.com/bibenga/barn-go"
	"github.com/google/uuid"
)

type LockerConfig struct {
	Log        *slog.Logger
	Name       string
	Repository LockRepository
	LockName   string
	Ttl        time.Duration
	Hearbeat   time.Duration
}

// Locker is the mutex entry in the database.
type Locker struct {
	log        *slog.Logger
	name       string
	db         *sql.DB
	repository LockRepository
	lockName   string
	ttl        time.Duration
	hearbeat   time.Duration
	locked     bool
	lockedAt   *time.Time
}

// The default TTL is 60 sec
const DefaultLockTtl = 60 * time.Second

// The defaulth heartbeat interval is 20 sec
const DefaultHeartbeat = DefaultLockTtl / 3

// NewLockWithConfig create new Lock with config, during initialization config is adjusted
func NewLockWithConfig(db *sql.DB, config *LockerConfig) *Locker {
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
			if uuid, err := uuid.NewRandom(); err != nil {
				panic(err)
			} else {
				name = uuid.String()
			}
		} else {
			name = fmt.Sprintf("%s-%d", name, os.Getpid())
		}
		config.Name = name
	}
	if config.Repository == nil {
		config.Repository = NewDefaultPostgresLockRepository()
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
		config.Log = slog.Default()
	}

	lock := &Locker{
		log:        config.Log,
		name:       config.Name,
		db:         db,
		repository: config.Repository,
		lockName:   config.LockName,
		ttl:        config.Ttl,
		hearbeat:   config.Hearbeat,
		locked:     false,
		lockedAt:   nil,
	}
	return lock
}

func NewDefaultLock(db *sql.DB) *Locker {
	return NewLockWithConfig(db, &LockerConfig{})
}

func (l *Locker) Name() string {
	return l.name
}

func (l *Locker) LockName() string {
	return l.lockName
}

func (l *Locker) Ttl() time.Duration {
	return l.ttl
}

func (l *Locker) Hearbeat() time.Duration {
	return l.hearbeat
}

func (l *Locker) IsLocked() bool {
	return l.locked
}

func (l *Locker) LockedAt() *time.Time {
	return l.lockedAt
}

// Try to acquire the lock
func (l *Locker) TryLock() (bool, error) {
	if l.locked {
		return false, errors.New("codebug: the lock is locked")
	}
	l.log.Debug("TryLock")
	err := barngo.RunInTransaction(l.db, func(tx *sql.Tx) error {
		lock, err := l.repository.FindOne(tx, l.lockName)
		if err != nil {
			return err
		}
		l.log.Info("the lock is loaded", "lock", lock)
		if lock == nil {
			if err := l.repository.Create(tx, l.lockName); err != nil {
				return err
			}
			lock, err = l.repository.FindOne(tx, l.lockName)
			if err != nil {
				return err
			}
		}
		now := time.Now().UTC()
		rotten := now.Add(-l.ttl)
		if lock.LockedAt == nil || lock.LockedAt.Before(rotten) {
			lock.LockedAt = &now
			lock.Owner = &l.name
			l.log.Info("the lock is acquired", "lock", lock)
			if err = l.repository.Save(tx, lock); err != nil {
				return err
			}
			l.locked = true
			l.lockedAt = lock.LockedAt
		}
		return nil
	})
	return l.locked, err
}

// Acquire the lock
func (l *Locker) Lock(interval time.Duration) (bool, error) {
	return l.LockContext(context.Background(), interval)
}

// Acquire the lock
func (l *Locker) LockContext(ctx context.Context, interval time.Duration) (bool, error) {
	if l.locked {
		return false, errors.New("codebug: the lock is locked")
	}
	l.log.Debug("LockContext", "interval", interval)
	if locked, err := l.TryLock(); err != nil {
		return false, err
	} else {
		if locked {
			return locked, nil
		}
	}

	ticker := time.NewTicker(interval)
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
func (l *Locker) Confirm() (bool, error) {
	if !l.locked {
		return false, errors.New("codebug: the lock is not locked")
	}
	l.log.Debug("Confirm")
	err := barngo.RunInTransaction(l.db, func(tx *sql.Tx) error {
		lock, err := l.repository.FindOne(tx, l.lockName)
		if err != nil {
			return err
		}
		if lock == nil {
			return sql.ErrNoRows
		}
		l.log.Info("the lock is loaded", "lock", lock)
		now := time.Now().UTC()
		rotten := now.Add(-l.ttl)
		if lock.LockedAt != nil && lock.LockedAt.After(rotten) && lock.Owner != nil && *lock.Owner == l.name {
			lock.LockedAt = &now
			l.log.Info("the lock is confirmed", "lock", lock)
			if err = l.repository.Save(tx, lock); err != nil {
				return err
			}
			l.lockedAt = lock.LockedAt
		} else {
			l.log.Warn("the lock was recaptured by someone")
			l.locked = false
			l.lockedAt = nil
		}
		return nil
	})
	return l.locked, err
}

// Release lock
func (l *Locker) Unlock() (bool, error) {
	if !l.locked {
		return false, errors.New("codebug: the lock is not locked")
	}
	unlocked := false
	err := barngo.RunInTransaction(l.db, func(tx *sql.Tx) error {
		lock, err := l.repository.FindOne(tx, l.lockName)
		if err != nil {
			return err
		}
		if lock == nil {
			return sql.ErrNoRows
		}
		l.log.Info("the lock is loaded", "lock", lock)
		now := time.Now().UTC()
		rotten := now.Add(-l.ttl)
		if lock.LockedAt != nil && lock.LockedAt.After(rotten) && lock.Owner != nil && *lock.Owner == l.name {
			lock.LockedAt = nil
			lock.Owner = nil
			l.log.Info("the lock is released", "lock", lock)
			if err = l.repository.Save(tx, lock); err != nil {
				return err
			}
			unlocked = true
		} else {
			l.log.Warn("the lock was recaptured by someone")
		}
		l.locked = false
		l.lockedAt = nil
		return nil
	})
	return unlocked, err
}
