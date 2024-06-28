package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/scheduler"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := scheduler.NewPostgresSchedulerRepository()
	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		r := repository.(*scheduler.PostgresSchedulerRepository)
		if err := r.CreateTable(tx); err != nil {
			return err
		}
		if err := r.DeleteAll(tx); err != nil {
			return err
		}

		cron1 := "*/5 * * * * *"
		message1 := "{\"type\":\"olala1\"}"
		if err := r.Create(tx, &scheduler.Schedule{Name: "olala1", Cron: &cron1, Payload: &message1}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	scheduler := scheduler.NewScheduler(db, &scheduler.SchedulerConfig{Repository: repository})
	scheduler.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	scheduler.Stop()
}
