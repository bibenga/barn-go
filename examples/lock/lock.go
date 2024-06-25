package main

import (
	"database/sql"
	"errors"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/lock"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := lock.NewDefaultPostgresLockRepository()
	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		pgRepository := repository.(*lock.PostgresLockRepository)
		if err := pgRepository.CreateTable(tx); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	l := lock.NewLockWithConfig(db, &lock.LockerConfig{
		Repository: repository,
	})
	if locked, err := l.TryLock(); err != nil {
		panic(err)
	} else {
		if !locked {
			panic(errors.New("is not locked"))
		}
	}
	if unlocked, err := l.Unlock(); err != nil {
		panic(err)
	} else {
		if !unlocked {
			panic(errors.New("is not unlocked"))
		}
	}
}
