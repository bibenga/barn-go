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

	sched := scheduler.NewSimpleScheduler(db, &scheduler.SimpleSchedulerConfig{
		Cron: "*/5 * * * * *",
	})
	if err := sched.CreateTable(); err != nil {
		panic(err)
	}
	// if err := sched.DeleteAll(); err != nil {
	// 	panic(err)
	// }

	// cron1 := "*/5 * * * * *"
	// message1 := "{\"type\":\"olala1\"}"
	// if err := sched.Add(&scheduler.Task{Name: "olala1", Cron: &cron1, Message: &message1}); err != nil {
	// 	panic(err)
	// }

	ctx, cancel := context.WithCancel(context.Background())

	sched.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	sched.Stop()
}
