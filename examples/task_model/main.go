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

type TaskPay struct {
	// barngo.Task
	Id         int           `barn:""`
	RunAt      time.Time     `barn:""`
	Status     barngo.Status `barn:""`
	StartedAt  *time.Time    `barn:""`
	FinishedAt *time.Time    `barn:""`
	Error      *string       `barn:""`

	User           string  `barn:"user_id"`
	Amount         float64 `barn:""`
	IdempotencyKey string  `barn:""`
	Attemt         int     `barn:""`
	MaxAttempts    int     `barn:""`
}

func (e TaskPay) TableName() string {
	return "barn_task_pay"
}

func main() {
	examples.Setup(true)

	db := examples.InitDb(true, "examples/task_model/schema.sql")
	defer db.Close()

	var worker *barngo.Worker[TaskPay]
	worker = barngo.NewWorker[TaskPay](
		db,
		barngo.WorkerConfig[TaskPay]{
			Cron: "*/5 * * * * *",
			Handler: func(tx *sql.Tx, task *TaskPay) (any, error) {
				if task.Attemt < task.MaxAttempts {
					task1 := TaskPay{
						RunAt:          time.Now().Add(5 * time.Second),
						User:           task.User,
						Amount:         task.Amount,
						IdempotencyKey: task.IdempotencyKey,
						Attemt:         task.Attemt + 1,
						MaxAttempts:    task.MaxAttempts,
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
		task1 := TaskPay{
			// Func: "sentEmail",
			// Args: map[string]any{"str": "str", "int": 12},
			User:           "1360b505-b41f-45c1-966b-eb71c775604b",
			Amount:         100,
			IdempotencyKey: "social-romeo-river-king",
			Attemt:         1,
			MaxAttempts:    10,
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
