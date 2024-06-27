package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"time"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/lock"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := lock.NewPostgresLockRepository()
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

	leaderLock := lock.NewLockWithConfig(db, &lock.LockerConfig{
		Repository: repository,
		Ttl:        10 * time.Second,
		Hearbeat:   1 * time.Second,
	})
	leader := lock.NewLeaderElector(&lock.LeaderElectorConfig{
		Lock: leaderLock,
	})

	ctx, cancel := context.WithCancel(context.Background())

	leader.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()

	leader.Stop()
}
