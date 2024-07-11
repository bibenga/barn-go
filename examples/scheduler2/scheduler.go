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

	worker := scheduler.NewSimpleScheduler2[scheduler.Schedule](
		db,
		scheduler.SchedulerConfig2[scheduler.Schedule]{
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

		cron1 := "*/5 * * * * *"
		schedule := scheduler.Schedule{
			Name:     "olala1",
			IsActive: true,
			Cron:     &cron1,
			Func:     "sendEmails",
			Args:     map[string]any{"type": "welcome"},
		}
		if err := worker.Create(tx, &schedule); err != nil {
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
