package barn

import (
	"database/sql"
	"log/slog"
	"time"

	"github.com/bibenga/barn-go/internal/adapter"

	"github.com/google/uuid"
)

type Lock struct {
	Name     string
	LockedAt sql.NullTime
	LockedBy sql.NullString
}

type LockManager struct {
	log           *slog.Logger
	db            *sql.DB
	query         *adapter.LockQuery
	hostname      string
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
	OnCaptured(lockName string)
	OnReleased(lockName string)
}

func NewLockManager(db *sql.DB, listener LockListener) *LockManager {
	uuid, _ := uuid.NewRandom()
	hostname := uuid.String()
	lockName := "barn"
	manager := LockManager{
		log:           slog.Default().With("lock", lockName, "hostname", hostname),
		db:            db,
		query:         adapter.NewDefaultLockQuery(),
		listener:      listener,
		hostname:      hostname,
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
	if manager.isLockExist() {
		manager.log.Info("the lock exists")
	} else {
		manager.create()
	}

	manager.check()

	check := time.NewTicker(manager.checkInterval)
	defer check.Stop()

	manager.log.Info("started")
	for {
		select {
		case <-manager.stop:
			manager.log.Info("terminate")
			manager.stopped <- struct{}{}
			if manager.isLocked {
				manager.release()
			}
			return
		case <-check.C:
			manager.check()
		}
	}
}

func (manager *LockManager) check() {
	// dbLock := manager.getDbLock()
	// if manager.isLocked {
	// 	if dbLock.LockedBy.Valid && dbLock.LockedBy.String == manager.hostname {
	// 		if manager.tryConfirmOrCapture() {
	// 			manager.log.Info("the lock is still captured")
	// 		} else {
	// 			manager.log.Warn("the lock was captured unexpectedly by someone")
	// 			manager.isLocked = false
	// 			manager.onReleased()
	// 		}
	// 	} else {
	// 		manager.log.Warn("the lock was captured by someone")
	// 		manager.isLocked = false
	// 		manager.onReleased()
	// 	}
	// } else if !dbLock.LockedAt.Valid || time.Since(dbLock.LockedAt.Time) > manager.expiration {
	// 	manager.log.Info("the lock is rotten")
	// 	if manager.tryConfirmOrCapture() {
	// 		manager.isLocked = true
	// 		manager.onCaptured()
	// 	}
	// }

	if manager.isLocked {
		if manager.confirm() {
			manager.log.Info("the lock is still captured")
		} else {
			manager.log.Warn("the lock was captured unexpectedly")
			manager.isLocked = false
			manager.onReleased()
		}
	} else {
		if manager.tryCapture() {
			manager.log.Info("the lock is rotten and is captured")
			manager.isLocked = true
			manager.onCaptured()
		} else {
			manager.log.Info("the lock is alive")
		}
	}
}

func (manager *LockManager) isLockExist() bool {
	db := manager.db
	stmt, err := db.Prepare(manager.query.GetIsExistQuery())
	if err != nil {
		manager.log.Error("cannot prepare query", "error", err)
		panic(err)
	}
	defer stmt.Close()
	var count int
	row := stmt.QueryRow(manager.lockName)
	switch err := row.Scan(&count); err {
	case sql.ErrNoRows:
		manager.log.Info("the lock is not exist")
		return false
	case nil:
		manager.log.Info("the lock is exist")
		return true
	default:
		manager.log.Error("db error", "error", err)
		panic(err)
	}
}

func (manager *LockManager) create() {
	db := manager.db
	manager.log.Info("create the lock")

	tx, err := db.Begin()
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}

	res, err := tx.Exec(
		manager.query.GetInsertQuery(),
		manager.lockName,
	)
	if err != nil {
		manager.log.Error("cannot create lock", "error", err)
		panic(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}
	if rowsAffected == 1 {
		manager.log.Info("the lock is created")
	} else {
		manager.log.Info("the lock was created by someone")
	}

	err = tx.Commit()
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}
}

func (manager *LockManager) getDbLock() *Lock {
	db := manager.db
	stmt, err := db.Prepare(manager.query.GetSelectQuery())
	if err != nil {
		manager.log.Error("cannot prepare query", "error", err)
		panic(err)
	}
	defer stmt.Close()
	var dbLock Lock = Lock{Name: manager.lockName}
	row := stmt.QueryRow(manager.lockName)
	switch err := row.Scan(&dbLock.LockedAt, &dbLock.LockedBy); err {
	case sql.ErrNoRows:
		manager.log.Info("the lock is not found")
		return nil
	case nil:
		manager.log.Info("the lock is found", "LockedBy", dbLock.LockedBy.String)
		return &dbLock
	default:
		manager.log.Error("db error", "error", err)
		panic(err)
	}
}

func (manager *LockManager) confirm() bool {
	db := manager.db
	lockedAt := time.Now().UTC()
	rottenTs := time.Now().UTC().Add(-manager.expiration)
	// manager.log.Info("try capture lock", "rottenTs", rottenTs)
	res, err := db.Exec(
		manager.query.GetConfirmQuery(),
		manager.hostname, lockedAt,
		manager.lockName, manager.hostname, rottenTs,
	)
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}
	return rowsAffected == 1
}

func (manager *LockManager) tryCapture() bool {
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
		panic(err)
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		manager.log.Error("db error", "error", err)
		panic(err)
	}
	return rowsAffected == 1
}

func (manager *LockManager) release() {
	if manager.isLocked {
		db := manager.db
		rottenTs := time.Now().UTC().Add(-manager.expiration)
		res, err := db.Exec(
			manager.query.GetUnlockQuery(),
			manager.lockName, manager.hostname, rottenTs,
		)
		if err != nil {
			manager.log.Error("db error", "error", err)
			panic(err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			manager.log.Error("db error", "error", err)
			panic(err)
		}
		if rowsAffected == 1 {
			manager.log.Info("the lock is released")
		} else {
			manager.log.Info("the lock was created by someone")
		}
	}
}

func (manager *LockManager) onCaptured() {
	manager.listener.OnCaptured(manager.lockName)
}

func (manager *LockManager) onReleased() {
	manager.listener.OnReleased(manager.lockName)
}

type DummyLockListener struct{}

func (l *DummyLockListener) OnCaptured(lockName string) {
	slog.Info("DUMMY: the lock is captured", "lock", lockName)
}

func (l *DummyLockListener) OnReleased(lockName string) {
	slog.Info("DUMMY: the lock is released", "lock", lockName)
}

var _ LockListener = &DummyLockListener{}
