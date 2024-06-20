package barn

import (
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/bibenga/barn-go/internal/adapter"

	"github.com/google/uuid"
)

type Lock struct {
	Name     string
	LockedAt *time.Time
	LockedBy *string
}

type LockManager struct {
	log           *slog.Logger
	hostname      string
	db            *sql.DB
	query         adapter.LockQuery
	listener      LockListener
	lockName      string
	checkInterval time.Duration
	expiration    time.Duration
	isLocked      bool
	lockedAt      *time.Time
	stop          chan struct{}
	stopped       chan struct{}
}

type LockListener interface {
	OnAcquire(lockName string)
	OnRelease(lockName string)
}

func NewLockManager(db *sql.DB, listener LockListener) *LockManager {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Warn("can't get hostname", "error", err)
		uuid, _ := uuid.NewRandom()
		hostname = uuid.String()
	}

	lockName := "barn"

	manager := LockManager{
		log:           slog.Default().With("lock", lockName, "hostname", hostname),
		hostname:      hostname,
		db:            db,
		query:         adapter.NewDefaultLockQuery(),
		listener:      listener,
		lockName:      lockName,
		checkInterval: 1 * time.Second,
		expiration:    10 * time.Second,
		isLocked:      false,
		stop:          make(chan struct{}),
		stopped:       make(chan struct{}),
	}
	return &manager
}

func (manager *LockManager) InitializeDB() error {
	db := manager.db
	manager.log.Warn("create lock table")
	_, err := db.Exec(manager.query.GetCreateQuery())
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
	if isLockExist, err := manager.isLockExist(); err != nil {
		panic(err)
	} else {
		if isLockExist {
			manager.log.Info("the lock exists")
		} else {
			if err := manager.create(); err != nil {
				panic(err)
			}
		}
	}

	if err := manager.check(); err != nil {
		panic(err)
	}

	check := time.NewTicker(manager.checkInterval)
	defer check.Stop()

	manager.log.Info("started")
	for {
		select {
		case <-manager.stop:
			manager.log.Info("terminate")
			manager.stopped <- struct{}{}
			if manager.isLocked {
				if released, err := manager.release(); err != nil {
					panic(err)
				} else {
					if released {
						manager.onRelease()
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
	if manager.isLocked {
		if confirmed, err := manager.confirm(); err != nil {
			return err
		} else {
			if confirmed {
				manager.log.Info("the lock is still owned me")
			} else {
				manager.log.Warn("the lock has been acquired by someone unexpectedly")
				manager.onRelease()
			}
		}
	} else {
		if acquired, err := manager.acquire(); err != nil {
			return err
		} else {
			if acquired {
				manager.log.Info("the lock is rotten and acquired")
				manager.onAcquire()
			} else {
				return manager.logState()
			}
		}
	}
	return nil
}

func (manager *LockManager) isLockExist() (bool, error) {
	db := manager.db
	stmt, err := db.Prepare(manager.query.GetIsExistQuery())
	if err != nil {
		manager.log.Error("cannot prepare query", "error", err)
		return false, err
	}
	defer stmt.Close()
	var count int
	row := stmt.QueryRow(manager.lockName)
	switch err := row.Scan(&count); err {
	case sql.ErrNoRows:
		manager.log.Info("the lock is not exist")
		return false, nil
	case nil:
		manager.log.Info("the lock is exist")
		return true, nil
	default:
		manager.log.Error("db error", "error", err)
		return false, err
	}
}

func (manager *LockManager) create() error {
	db := manager.db
	manager.log.Info("create the lock")

	tx, err := db.Begin()
	if err != nil {
		manager.log.Error("db error", "error", err)
		return err
	}

	res, err := tx.Exec(
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

	if err := tx.Commit(); err != nil {
		manager.log.Error("db error", "error", err)
		return err
	}
	return nil
}

func (manager *LockManager) acquire() (bool, error) {
	db := manager.db
	lockedAt := time.Now().UTC()
	rottenTs := time.Now().UTC().Add(-manager.expiration)
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
			manager.isLocked = true
			manager.lockedAt = &lockedAt
		}
		return manager.isLocked, nil
	}
}

func (manager *LockManager) confirm() (bool, error) {
	db := manager.db
	lockedAt := time.Now().UTC()
	rottenTs := time.Now().UTC().Add(-manager.expiration)
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
			manager.isLocked = false
			manager.lockedAt = nil
		}
		return manager.isLocked, nil
	}
}

func (manager *LockManager) release() (bool, error) {
	if manager.isLocked {
		db := manager.db
		rottenTs := time.Now().UTC().Add(-manager.expiration)
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
			manager.isLocked = false
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
	var dbLock Lock = Lock{Name: manager.lockName}
	row := stmt.QueryRow(manager.lockName)
	switch err := row.Scan(&dbLock.LockedAt, &dbLock.LockedBy); err {
	case sql.ErrNoRows:
		manager.log.Info("the lock is not found")
		return nil
	case nil:
		lockedAt := slog.Any("LockedAt", nil)
		if dbLock.LockedAt != nil {
			// lockedAt = slog.Any("LockedAt", *dbLock.LockedAt)
			lockedAt.Value = slog.TimeValue(*dbLock.LockedAt)
		}
		lockedBy := slog.Any("LockedBy", nil)
		if dbLock.LockedBy != nil {
			// lockedBy = slog.Any("LockedBy", *dbLock.LockedBy)
			lockedBy.Value = slog.StringValue(*dbLock.LockedBy)
		}
		// manager.log.Info("the lock is owned by someone", "LockedAt", dbLock.LockedAt, "LockedBy", dbLock.LockedBy)
		manager.log.Info("the lock is captured", lockedAt, lockedBy)
		return nil
	default:
		manager.log.Error("db error", "error", err)
		return err
	}
}

func (manager *LockManager) onAcquire() {
	manager.listener.OnAcquire(manager.lockName)
}

func (manager *LockManager) onRelease() {
	manager.listener.OnRelease(manager.lockName)
}

type DummyLockListener struct{}

func (l *DummyLockListener) OnAcquire(lockName string) {
	slog.Info("DUMMY: the lock is acquired", "lock", lockName)
}

func (l *DummyLockListener) OnRelease(lockName string) {
	slog.Info("DUMMY: the lock is released", "lock", lockName)
}

var _ LockListener = &DummyLockListener{}
