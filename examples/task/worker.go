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

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	worker := barngo.NewWorker[barngo.Task](
		db,
		barngo.WorkerConfig[barngo.Task]{
			Cron: "*/5 * * * * *",
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		if err := worker.CreateTable(tx); err != nil {
			return err
		}
		if err := worker.DeleteAll(tx); err != nil {
			return err
		}

		task1 := barngo.Task{
			Func: "sentEmail",
			Args: map[string]any{"str": "str", "int": 12},
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
