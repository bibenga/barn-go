package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"time"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
)

func main() {
	examples.Setup(true)

	db := examples.InitDb(true, "")
	defer db.Close()

	var worker *barngo.Worker[barngo.Task]
	worker = barngo.NewWorker[barngo.Task](
		db,
		barngo.WorkerConfig[barngo.Task]{
			Cron: "*/5 * * * * *",
			Handler: func(tx *sql.Tx, task *barngo.Task) (any, error) {
				// args := task.Args.(map[string]any)
				args := task.Args
				user := args["User"].(string)
				amount := args["Amount"].(float64)
				idempotencyKey := args["IdempotencyKey"].(string)
				attempt := int(args["Attemt"].(float64))
				maxAttempts := int(args["MaxAttempts"].(float64))
				if attempt < maxAttempts {
					task1 := barngo.Task{
						RunAt: time.Now().Add(5 * time.Second),
						Func:  "pay",
						Args: map[string]any{
							"User":           user,
							"Amount":         amount,
							"IdempotencyKey": idempotencyKey,
							"Attemt":         attempt + 1,
							"MaxAttempts":    maxAttempts,
						},
					}
					if err := worker.Create(tx, &task1); err != nil {
						return nil, err
					}
					return nil, errors.New("network error")
				}
				return nil, nil
			},
		},
	)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		task1 := barngo.Task{
			Func: "pay",
			Args: map[string]any{
				"User":           "1360b505-b41f-45c1-966b-eb71c775604b",
				"Amount":         200,
				"IdempotencyKey": "social-romeo-river-king",
				"Attemt":         1,
				"MaxAttempts":    10,
			},
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
