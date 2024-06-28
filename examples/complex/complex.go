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

	lockRepository := lock.NewPostgresLockRepository(
		lock.LockQueryConfig{
			TableName: "barn.lock",
		},
	)
	schedulerRepository := scheduler.NewPostgresSchedulerRepository(
		scheduler.ScheduleQueryConfig{
			TableName: "barn.schedule",
		},
	)
	queueRepository := queue.NewPostgresQueueRepository(
		queue.QueueQueryConfig{
			TableName: "barn.queue",
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(`create schema if not exists barn`)
		if err != nil {
			return err
		}

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
		payload := map[string]any{"str": "str", "int": 12}
		if err := pgSchedulerRepository.Create(tx, &scheduler.Schedule{Name: "olala1", Cron: &cron1, Payload: payload}); err != nil {
			return err
		}

		pgQueueRepository := queueRepository.(*queue.PostgresQueueRepository)
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
		Handler: func(tx *sql.Tx, s *scheduler.Schedule) error {
			var payload map[string]interface{}
			if s.Payload != nil {
				payload = s.Payload.(map[string]interface{})
			} else {
				payload = make(map[string]interface{})
			}
			payload["_meta"] = map[string]interface{}{
				"schedule": s.Name,
				"moment":   s.NextRunAt,
			}
			return queueRepository.Create(tx, &queue.Message{
				CreatedAt: *s.NextRunAt,
				Payload:   payload,
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
		Cron:       "*/5 * * * * *",
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
