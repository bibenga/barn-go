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
)

var registry *barngo.TaskRegistry
var worker *barngo.Worker[barngo.Task]
var scheduler *barngo.Scheduler[barngo.Schedule]

func taskHandler(tx *sql.Tx, t *barngo.Task) (any, error) {
	result, err := registry.Call(tx, t.Func, t.Args)
	if err != nil {
		return nil, err
	}
	t.Result = result
	return barngo.IgnoreResult, nil
}

func scheduleHandler(tx *sql.Tx, s *barngo.Schedule) error {
	return worker.Create(tx, &barngo.Task{
		RunAt: *s.NextRunAt,
		Func:  s.Func,
		Args:  s.Args,
	})
}

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	registry = barngo.NewTaskRegistry()
	registry.Register("sendNotifications", func(tx *sql.Tx, args any) (any, error) {
		slog.Info("CALLED: sendNotifications", "args", args)
		return true, nil
	})

	scheduler = barngo.NewScheduler[barngo.Schedule](
		db,
		barngo.SchedulerConfig[barngo.Schedule]{
			Cron:    "*/10 * * * * *",
			Handler: scheduleHandler,
		},
	)

	worker = barngo.NewWorker[barngo.Task](
		db,
		barngo.WorkerConfig[barngo.Task]{
			Cron:    "*/10 * * * * *",
			Handler: taskHandler,
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		cron1 := "*/5 * * * * *"
		schedule := barngo.Schedule{
			Name: "sendNotifications",
			Cron: &cron1,
			Func: "sendNotifications",
			Args: map[string]any{"type": "welcome"},
		}
		if err := scheduler.Create(tx, &schedule); err != nil {
			return err
		}

		task := barngo.Task{
			Func: "sendNotifications",
			Args: map[string]any{"type": "started"},
		}
		if err := worker.Create(tx, &task); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	scheduler.StartContext(ctx)
	worker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)
	scheduler.Stop()
	worker.Stop()
}
