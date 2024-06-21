package lock

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
)

type LockManager struct {
	log      *slog.Logger
	hostname string
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

func NewLockManager(db *sql.DB, lockName string, listener LockListener) *LockManager {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Warn("can't get hostname", "error", err)
		uuid, err := uuid.NewRandom()
		if err != nil {
			panic(err)
		}
		hostname = uuid.String()
	}

	manager := LockManager{
		log:      slog.Default().With("lock", lockName, "hostname", hostname),
		hostname: hostname,
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
	return &manager
}

func (manager *LockManager) CreateTable() error {
	db := manager.db
	manager.log.Warn("create lock table")
	_, err := db.Exec(manager.query.GetCreateTableQuery())
	return err
}

func (manager *LockManager) Stop() {
	manager.log.Info("stopping")
	manager.stop <- struct{}{}
	<-manager.stopped
	close(manager.stop)
	close(manager.stopped)
	manager.log.Info("stopped")
}

func (manager *LockManager) Run() {
	if err := manager.create(); err != nil {
		panic(err)
	}

	if err := manager.check(); err != nil {
		panic(err)
	}

	check := time.NewTicker(manager.hearbeat)
	defer check.Stop()

	manager.log.Info("started")
	for {
		select {
		case <-manager.stop:
			manager.log.Info("terminate")
			manager.stopped <- struct{}{}
			if manager.locked {
				if released, err := manager.unlock(); err != nil {
					panic(err)
				} else {
					if released {
						manager.onUnlock()
					}
				}
			}
			return
		case <-check.C:
			if err := manager.check(); err != nil {
				panic(err)
			}
		}
	}
}

func (manager *LockManager) check() error {
	if manager.locked {
		if confirmed, err := manager.confirm(); err != nil {
			return err
		} else {
			if confirmed {
				manager.log.Info("the lock is still owned me")
			} else {
				manager.log.Warn("the lock has been acquired by someone unexpectedly")
				manager.onUnlock()
			}
		}
	} else {
		if acquired, err := manager.tryLock(); err != nil {
			return err
		} else {
			if acquired {
				manager.log.Info("the lock is rotten and acquired")
				manager.onLock()
			} else {
				return manager.logState()
			}
		}
	}
	return nil
}

func (manager *LockManager) create() error {
	db := manager.db
	manager.log.Info("create the lock")

	res, err := db.Exec(
		manager.query.GetInsertQuery(),
		manager.lockName,
	)
	if err != nil {
		manager.log.Error("cannot create lock", "error", err)
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		manager.log.Error("db error", "error", err)
		return err
	}
	manager.log.Debug("sql", "RowsAffected", rowsAffected)
	if rowsAffected == 1 {
		manager.log.Info("the lock is created")
	} else {
		manager.log.Info("the lock was created by someone")
	}
	return nil
}

func (manager *LockManager) tryLock() (bool, error) {
	db := manager.db
	lockedAt := time.Now().UTC()
	rottenTs := lockedAt.Add(-manager.ttl)
	// manager.log.Info("try capture lock", "rottenTs", rottenTs)
	res, err := db.Exec(
		manager.query.GetLockQuery(),
		manager.hostname, lockedAt,
		manager.lockName, rottenTs,
	)
	if err != nil {
		manager.log.Error("db error", "error", err)
		return false, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		manager.log.Error("db error", "error", err)
		return false, err
	} else {
		manager.log.Debug("sql", "RowsAffected", rowsAffected)
		if rowsAffected == 1 {
			manager.locked = true
			manager.lockedAt = &lockedAt
		}
		return manager.locked, nil
	}
}

func (manager *LockManager) lock(ctx context.Context) (bool, error) {
	manager.log.Info("lock")
	if locked, err := manager.tryLock(); err != nil {
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
			if locked, err := manager.tryLock(); err != nil {
				return false, err
			} else {
				return locked, nil
			}
		}
	}
}

func (manager *LockManager) confirm() (bool, error) {
	db := manager.db
	lockedAt := time.Now().UTC()
	rottenTs := time.Now().UTC().Add(-manager.ttl)
	res, err := db.Exec(
		manager.query.GetConfirmQuery(),
		manager.hostname, lockedAt,
		manager.lockName, manager.hostname, rottenTs,
	)
	if err != nil {
		manager.log.Error("db error", "error", err)
		return false, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		manager.log.Error("db error", "error", err)
		return false, err
	} else {
		manager.log.Debug("sql", "RowsAffected", rowsAffected)
		if rowsAffected == 1 {
			manager.lockedAt = &lockedAt
		} else {
			manager.locked = false
			manager.lockedAt = nil
		}
		return manager.locked, nil
	}
}

func (manager *LockManager) unlock() (bool, error) {
	if manager.locked {
		db := manager.db
		rottenTs := time.Now().UTC().Add(-manager.ttl)
		res, err := db.Exec(
			manager.query.GetUnlockQuery(),
			manager.lockName, manager.hostname, rottenTs,
		)
		if err != nil {
			manager.log.Error("db error", "error", err)
			return false, err
		}
		if rowsAffected, err := res.RowsAffected(); err != nil {
			manager.log.Error("db error", "error", err)
			return false, err
		} else {
			manager.log.Debug("sql", "RowsAffected", rowsAffected)
			manager.locked = false
			manager.lockedAt = nil
			if rowsAffected == 1 {
				manager.log.Info("the lock is released")
			} else {
				manager.log.Info("the lock was created by someone")
			}
			return rowsAffected == 1, nil
		}
	} else {
		manager.log.Info("the lock is not our")
	}
	return false, nil
}

func (manager *LockManager) logState() error {
	db := manager.db
	stmt, err := db.Prepare(manager.query.GetSelectQuery())
	if err != nil {
		manager.log.Error("cannot prepare query", "error", err)
		return err
	}
	defer stmt.Close()
	var lockedAt *time.Time
	var lockedBy *string
	row := stmt.QueryRow(manager.lockName)
	switch err := row.Scan(&lockedAt, &lockedBy); err {
	case sql.ErrNoRows:
		manager.log.Info("the lock is not found")
		return nil
	case nil:
		lockedAtAttr := slog.Any("LockedAt", nil)
		if lockedAt != nil {
			lockedAtAttr.Value = slog.TimeValue(*lockedAt)
		}
		lockedByAttr := slog.Any("LockedBy", nil)
		if lockedBy != nil {
			lockedByAttr.Value = slog.StringValue(*lockedBy)
		}
		manager.log.Info("the lock is captured", lockedAtAttr, lockedByAttr)
		return nil
	default:
		manager.log.Error("db error", "error", err)
		return err
	}
}

func (manager *LockManager) onLock() {
	manager.listener.OnLock()
}

func (manager *LockManager) onUnlock() {
	manager.listener.OnUnlock()
}

type DummyLockListener struct{}

func (l *DummyLockListener) OnLock() {
	slog.Info("DUMMY: the lock is acquired")
}

func (l *DummyLockListener) OnUnlock() {
	slog.Info("DUMMY: the lock is released")
}

var _ LockListener = &DummyLockListener{}
