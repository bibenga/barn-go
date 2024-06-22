package main

import (
	"context"
	"database/sql"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/bibenga/barn-go/lock"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
)

// const driver string = "sqlite3"
// const dsn string = "file:barn/_barn.db?cache=shared&mode=rwc&_journal_mode=WAL&_loc=UTC"

const driver string = "pgx"
const dsn string = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=barn TimeZone=UTC sslmode=disable"

func initDb(debug bool) *sql.DB {
	var connectionString string = dsn
	if debug {
		connConfig, err := pgx.ParseConfig(dsn)
		if err != nil {
			slog.Error("db config errorz", "error", err)
			panic(err)
		}
		connConfig.Tracer = &tracelog.TraceLog{
			Logger:   pgxslog.NewLogger(slog.Default()),
			LogLevel: tracelog.LogLevelDebug,
		}
		connectionString = stdlib.RegisterConnConfig(connConfig)
	}
	db, err := sql.Open(driver, connectionString)
	if err != nil {
		slog.Error("db error", "error", err)
		panic(err)
	}
	if err := db.Ping(); err != nil {
		slog.Error("db error", "error", err)
		panic(err)
	}
	return db
}

func main() {
	// time.Local = time.UTC
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile | log.Lmsgprefix)
	log.SetPrefix("")
	slog.SetLogLoggerLevel(slog.LevelDebug)

	ctx, cancel := context.WithCancel(context.Background())

	db := initDb(false)
	defer db.Close()

	dbLock := lock.NewLock(db)
	if err := dbLock.CreateTable(); err != nil {
		slog.Error("db error", "error", err)
		panic(err)
	}

	leader := lock.NewLeaderElector(dbLock, &lock.DummyLeaderListener{})
	leader.StartContext(ctx)

	osSignal := make(chan os.Signal, 1)
	signal.Notify(osSignal, os.Interrupt)
	s := <-osSignal
	slog.Info("os signal received", "signal", s)

	cancel()
	time.Sleep(1 * time.Second)

}
