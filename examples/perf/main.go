package main

import (
	"database/sql"
	"log/slog"
	"time"

	barngo "github.com/bibenga/barn-go"
	"github.com/bibenga/barn-go/examples"
)

const count = 1000
const progress = 100

func insert(db *sql.DB, repository *barngo.Worker[barngo.Task]) {
	started := time.Now().UTC()
	for i := 0; i < count; i++ {
		if progress > 0 {
			if i%progress == 0 {
				slog.Info("progress", "i", i)
			}
		}
		err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
			created := time.Now().UTC()
			m := barngo.Task{
				Func: "sendEmails",
				Args: map[string]any{"created": created, "i": i},
			}
			if err := repository.Create(tx, &m); err != nil {
				panic(err)
			}
			return nil
		})
		if err != nil {
			panic(err)
		}
	}
	finished := time.Now().UTC()
	d := finished.Sub(started)
	slog.Info("insert", "duration", d, "n/s", float64(count)/d.Seconds())
}

func process(db *sql.DB, repository *barngo.Worker[barngo.Task]) {
	started := time.Now().UTC()
	i := 0
	for {
		if progress > 0 {
			if i%progress == 0 {
				slog.Info("progress", "i", i)
			}
		}
		err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
			t, err := repository.FindNext(tx)
			if err != nil {
				return err
			}
			if t == nil {
				return sql.ErrNoRows
			}
			i++
			t.Status = barngo.Done
			if err := repository.Save(tx, t); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			if err == sql.ErrNoRows {
				break
			}
			panic(err)
		}
	}
	finished := time.Now().UTC()
	d := finished.Sub(started)
	slog.Info("process", "duration", d, "n/s", float64(count)/d.Seconds())
}

func main() {
	examples.Setup(true)

	db := examples.InitDb(false, "")
	defer db.Close()

	repository := barngo.NewWorker[barngo.Task](db)

	insert(db, repository)
	process(db, repository)
}
