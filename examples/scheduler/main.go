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

func main() {
	examples.Setup(true)

	db := examples.InitDb(false, "")
	defer db.Close()

	worker := barngo.NewScheduler[barngo.Schedule](
		db,
		barngo.SchedulerConfig[barngo.Schedule]{
			Cron: "*/5 * * * * *",
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		interval1 := 7 * time.Second
		// cron1 := "*/5 * * * * *"
		schedule := barngo.Schedule{
			Name:     "olala1",
			IsActive: true,
			Interval: &interval1,
			// Cron: &cron1,
			Func: "sendEmails",
			Args: map[string]any{"type": "welcome"},
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
