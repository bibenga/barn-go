package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"

	"github.com/bibenga/barn-go/examples"
	"github.com/bibenga/barn-go/scheduler"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(false)
	defer db.Close()

	repository := scheduler.NewDefaultPostgresSchedulerRepository()

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
