package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/lock"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	leaderLock := lock.NewLockWithConfig(db, &lock.LockConfig{
		Ttl:      10 * time.Second,
		Hearbeat: 1 * time.Second,
	})
	if err := leaderLock.CreateTable(); err != nil {
		panic(err)
	}
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
