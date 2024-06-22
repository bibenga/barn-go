package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/lock"
	"github.com/bibenga/barn-go/scheduler"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	sched := scheduler.NewScheduler(db, &scheduler.SchedulerConfig{
		Listener: &scheduler.DummySchedulerListener{},
	})
	if err := sched.CreateTable(); err != nil {
		panic(err)
	}
	if err := sched.DeleteAll(); err != nil {
		panic(err)
	}

	cron1 := "*/5 * * * * *"
	message1 := "{\"type\":\"olala1\"}"
	if err := sched.Add(&scheduler.Entry{Name: "olala1", Cron: &cron1, Message: &message1}); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var shedCtx context.Context
	var shedCtxCancel context.CancelFunc

	leaderLock := lock.NewLockWithConfig(db, &lock.LockConfig{
		Ttl:      10 * time.Second,
		Hearbeat: 1 * time.Second,
	})
	if err := leaderLock.CreateTable(); err != nil {
		panic(err)
	}
	leader := lock.NewLeaderElector(&lock.LeaderElectorConfig{
		Lock: leaderLock,
		OnElectedHandler: func() {
			shedCtx, shedCtxCancel = context.WithCancel(ctx)
			sched.StartContext(shedCtx)
		},
		OnUnelectedHandler: func() {
			if shedCtxCancel != nil {
				shedCtxCancel()
				shedCtxCancel = nil
			}
		},
	})

	leader.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)
}
