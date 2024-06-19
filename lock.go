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
	LockedAt time.Time
	LockedBy string
}

type LockManager struct {
	log           *slog.Logger
	db            *sql.DB
	query         *adapter.LockQuery
	hostname      string
	lockName      string
	checkInterval time.Duration
	expiration    time.Duration
	isLocked      bool
	stop          chan struct{}
	stopped       chan struct{}
}

func NewLockManager(db *sql.DB) *LockManager {
	uuid, _ := uuid.NewRandom()
	lockName := "barn"
	manager := LockManager{
		log:           slog.Default().With("lock", lockName),
		db:            db,
		query:         adapter.NewDefaultLockQuery(),
		hostname:      uuid.String(),
		lockName:      lockName,
		checkInterval: 5 * time.Second,
		expiration:    30 * time.Second,
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

	check := time.NewTicker(manager.checkInterval)
	defer check.Stop()

	manager.log.Info("started")
	for {
		select {
		case <-manager.stop:
			manager.log.Info("terminate")
			manager.stopped <- struct{}{}
			return
		case <-check.C:
			manager.check()
		}
	}
}

func (manager *LockManager) check() {
	dbLock := manager.getDbLock()
	if manager.isLocked {
		if dbLock.LockedBy == manager.hostname {
			if manager.tryUpdate(dbLock) {
				manager.log.Info("the lock is still captured")
			} else {
				manager.log.Warn("the lock was captured unexpectedly by someone")
				manager.isLocked = false
				manager.onReleased()
			}
		} else {
			manager.log.Warn("the lock was captured by someone")
			manager.isLocked = false
			manager.onReleased()
		}
	} else if time.Since(dbLock.LockedAt) > manager.expiration {
		manager.log.Info("the lock is rotten")
		if manager.tryUpdate(dbLock) {
			manager.isLocked = true
			manager.onCaptured()
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
		manager.lockName, time.Now().UTC().Add((-300*24)*time.Hour), "",
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
		manager.log.Info("the lock is found", "state", dbLock)
		return &dbLock
	default:
		manager.log.Error("db error", "error", err)
		panic(err)
	}
}

func (manager *LockManager) tryUpdate(dbLock *Lock) bool {
	db := manager.db
	res, err := db.Exec(
		manager.query.GetUpdateQuery(),
		time.Now().UTC(), manager.hostname,
		manager.lockName, dbLock.LockedAt, dbLock.LockedBy,
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

func (manager *LockManager) onCaptured() {
	manager.log.Info("lock is captured")
}

func (manager *LockManager) onReleased() {
	manager.log.Warn("lock is released")
}
