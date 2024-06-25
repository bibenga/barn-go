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
	"github.com/bibenga/barn-go/queue"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := queue.NewDefaultPostgresMessageRepository()

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		r := repository.(*queue.PostgresMessageRepository)
		if err := r.CreateTable(tx); err != nil {
			return err
		}
		if err := r.DeleteAll(tx); err != nil {
			return err
		}

		message1 := queue.Message{
			Payload:   "{}",
			CreatedTs: time.Now().UTC(),
		}
		if err := r.Create(tx, &message1); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	worker := queue.NewWorker(db, &queue.WorkerConfig{
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
