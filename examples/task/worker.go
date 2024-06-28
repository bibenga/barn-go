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
	"github.com/bibenga/barn-go/task"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := task.NewPostgresTaskRepository()

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		r := repository.(*task.PostgresTaskRepository)
		if err := r.CreateTable(tx); err != nil {
			return err
		}
		if err := r.DeleteAll(tx); err != nil {
			return err
		}

		// payload1, err := json.Marshal(map[string]any{"str": "str", "int": 12})
		// if err != nil {
		// return err
		// }
		task1 := task.Task{
			Func:      "sentEmail",
			Args:      map[string]any{"str": "str", "int": 12},
			CreatedAt: time.Now().UTC(),
		}
		if err := r.Create(tx, &task1); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	worker := task.NewWorker(db, &task.WorkerConfig{
		Repository: repository,
		Cron:       "*/5 * * * * *",
	})
	worker.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()

	worker.Stop()
}
