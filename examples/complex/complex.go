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
	"github.com/bibenga/barn-go/scheduler"
	"github.com/bibenga/barn-go/task"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	lockRepository := lock.NewPostgresLockRepository(
		lock.LockQueryConfig{
			TableName: "public.lock",
		},
	)
	schedulerRepository := scheduler.NewPostgresSchedulerRepository(
		scheduler.ScheduleQueryConfig{
			TableName: "public.schedule",
		},
	)
	taskRepository := task.NewPostgresTaskRepository(
		task.TaskQueryConfig{
			TableName: "public.task",
		},
	)
	registry := task.NewRegistry()

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
		schedule := scheduler.Schedule{
			Name: "sendNotifications",
			Cron: &cron1,
			Func: "sendNotifications",
			Args: map[string]any{"type": "welcome"},
		}
		if err := pgSchedulerRepository.Create(tx, &schedule); err != nil {
			return err
		}

		pgTaskRepository := taskRepository.(*task.PostgresTaskRepository)
		if err := pgTaskRepository.CreateTable(tx); err != nil {
			return err
		}
		task := task.Task{
			Func: "sendNotifications",
			Args: map[string]any{"type": "started"},
		}
		if err := pgTaskRepository.Create(tx, &task); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	registry.Register("sendNotifications", func(tx *sql.Tx, args any) (any, error) {
		slog.Info("CALLED: sendNotifications", "args", args)
		return true, nil
	})

	ctx, cancel := context.WithCancel(context.Background())

	scheduler := scheduler.NewScheduler(db, &scheduler.SchedulerConfig{
		Repository: schedulerRepository,
		Handler: func(tx *sql.Tx, s *scheduler.Schedule) error {
			err = taskRepository.Create(tx, &task.Task{
				CreatedAt: *s.NextRunAt,
				Func:      s.Func,
				Args:      s.Args,
			})
			if err != nil {
				return err
			}
			return nil
		},
	})

	electorLock := lock.NewLockWithConfig(db, &lock.LockerConfig{
		Repository: lockRepository,
		Ttl:        10 * time.Second,
		Hearbeat:   1 * time.Second,
	})
	elector := lock.NewLeaderElector(&lock.LeaderElectorConfig{
		Lock: electorLock,
		Handler: func(leader bool) error {
			if leader {
				scheduler.StartContext(ctx)
			} else {
				scheduler.Stop()
			}
			return nil
		},
	})

	elector.StartContext(ctx)

	worker := task.NewWorker(db, &task.WorkerConfig{
		Repository: taskRepository,
		Cron:       "*/10 * * * * *",
		Handler: func(tx *sql.Tx, t *task.Task) error {
			result, err := registry.Call(tx, t.Func, t.Args)
			if err != nil {
				return err
			}
			t.Result = result
			return nil
		},
	})
	worker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)

	elector.Stop()
	scheduler.Stop()
	worker.Stop()
}
