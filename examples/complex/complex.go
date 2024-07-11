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
	"github.com/bibenga/barn-go/scheduler"
	"github.com/bibenga/barn-go/task"
)

var sworker *scheduler.Scheduler2[scheduler.Schedule]
var tworker *task.Worker2[task.Task]
var registry *task.TaskRegistry

func scheduleHandler(tx *sql.Tx, s *scheduler.Schedule) error {
	return tworker.Create(tx, &task.Task{
		RunAt: *s.NextRunAt,
		Func:  s.Func,
		Args:  s.Args,
	})
}

func taskHandler(tx *sql.Tx, t *task.Task) (any, error) {
	result, err := registry.Call(tx, t.Func, t.Args)
	if err != nil {
		return nil, err
	}
	t.Result = result
	return task.IgnoreResult, nil
}

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	registry = task.NewTaskRegistry()
	registry.Register("sendNotifications", func(tx *sql.Tx, args any) (any, error) {
		slog.Info("CALLED: sendNotifications", "args", args)
		return true, nil
	})

	sworker := scheduler.NewSimpleScheduler2[scheduler.Schedule](
		db,
		scheduler.SchedulerConfig2[scheduler.Schedule]{
			Cron:    "*/10 * * * * *",
			Handler: scheduleHandler,
		},
	)

	tworker = task.NewWorker2[task.Task](
		db,
		task.WorkerConfig2[task.Task]{
			Cron:    "*/10 * * * * *",
			Handler: taskHandler,
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(`create schema if not exists barn`)
		if err != nil {
			return err
		}

		if err := sworker.CreateTable(tx); err != nil {
			return err
		}
		if err := sworker.DeleteAll(tx); err != nil {
			return err
		}

		cron1 := "*/5 * * * * *"
		schedule := scheduler.Schedule{
			Name: "sendNotifications",
			Cron: &cron1,
			Func: "sendNotifications",
			Args: map[string]any{"type": "welcome"},
		}
		if err := sworker.Create(tx, &schedule); err != nil {
			return err
		}

		if err := tworker.CreateTable(tx); err != nil {
			return err
		}
		task := task.Task{
			Func: "sendNotifications",
			Args: map[string]any{"type": "started"},
		}
		if err := tworker.Create(tx, &task); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	sworker.StartContext(ctx)
	tworker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)
	sworker.Stop()
	tworker.Stop()
}
