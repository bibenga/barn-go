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
	"github.com/bibenga/barn-go/queue"
	"github.com/bibenga/barn-go/scheduler"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	lockRepository := lock.NewDefaultPostgresLockRepository()
	schedulerRepository := scheduler.NewDefaultPostgresSchedulerRepository()
	queueRepository := queue.NewDefaultPostgresMessageRepository()

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		pgLockRepository := lockRepository.(*lock.PostgresLockRepository)
		if err := pgLockRepository.CreateTable(tx); err != nil {
			return err
		}

		pgSchedulerRepository := schedulerRepository.(*scheduler.PostgresSchedulerRepository)
		if err := pgSchedulerRepository.CreateTable(tx); err != nil {
			return err
		}
		if err := pgSchedulerRepository.DeleteAll(tx); err != nil {
			return err
		}

		cron1 := "*/5 * * * * *"
		message1 := "{\"type\":\"olala1\"}"
		if err := pgSchedulerRepository.Create(tx, &scheduler.Schedule{Name: "olala1", Cron: &cron1, Message: &message1}); err != nil {
			return err
		}

		// cron2 := "*/5 * * * * *"
		// nextTs2 := time.Now().UTC().Add(-20 * time.Second)
		// if err := r.Create(tx, &scheduler.Schedule{Name: "olala2", Cron: &cron2, NextTs: &nextTs2}); err != nil {
		// 	return err
		// }

		pgQueueRepository := queueRepository.(*queue.PostgresMessageRepository)
		if err := pgQueueRepository.CreateTable(tx); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	scheduler := scheduler.NewScheduler(db, &scheduler.SchedulerConfig{
		Repository: schedulerRepository,
		Handler: func(tx *sql.Tx, name string, moment time.Time, message *string) error {
			return queueRepository.Create(tx, &queue.Message{
				Created: moment,
				Payload: *message,
			})
		},
	})

	leaderLock := lock.NewLockWithConfig(db, &lock.LockerConfig{
		Repository: lockRepository,
		Ttl:        10 * time.Second,
		Hearbeat:   1 * time.Second,
	})
	leader := lock.NewLeaderElector(&lock.LeaderElectorConfig{
		Lock: leaderLock,
		Handler: func(leader bool) error {
			if leader {
				scheduler.StartContext(ctx)
			} else {
				scheduler.Stop()
			}
			return nil
		},
	})

	leader.StartContext(ctx)

	worker := queue.NewWorker(db, &queue.WorkerConfig{
		Repository: queueRepository,
		Cron:       "*/20 * * * * *",
	})
	worker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)

	leader.Stop()
	scheduler.Stop()
	worker.Stop()
}
