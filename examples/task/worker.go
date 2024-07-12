package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
)

type ExampleTask struct {
	barngo.Task
	Attempt     int `barn:""`
	MaxAttempts int `barn:""`
}

func main() {
	examples.Setup(true)

	db := examples.InitDb(true, "")
	defer db.Close()

	worker := barngo.NewWorker[ExampleTask](
		db,
		barngo.WorkerConfig[ExampleTask]{
			Cron: "*/5 * * * * *",
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		task1 := ExampleTask{
			Task: barngo.Task{
				Func: "sentEmail",
				Args: map[string]any{"str": "str", "int": 12},
			},
			// Func: "sentEmail",
			// Args: map[string]any{"str": "str", "int": 12},
		}
		if err := worker.Create(tx, &task1); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	worker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()

	worker.Stop()
}
