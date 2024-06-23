package examples

import (
	"database/sql"
	"log"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
)

const DriverName = "pgx"
const ConnectionString = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=barn TimeZone=UTC sslmode=disable"

func Setup(trace bool) {
	// time.Local = time.UTC
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile | log.Lmsgprefix)
	log.SetPrefix("")
	if trace {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}
}

func InitDb(trace bool) *sql.DB {
	connectionString := ConnectionString
	if trace {
		config, err := pgx.ParseConfig(connectionString)
		if err != nil {
			panic(err)
		}
		config.Tracer = &tracelog.TraceLog{
			Logger:   pgxslog.NewLogger(slog.Default()),
			LogLevel: tracelog.LogLevelDebug,
		}
		connectionString = stdlib.RegisterConnConfig(config)
	}
	db, err := sql.Open(DriverName, connectionString)
	if err != nil {
		panic(err)
	}
	if err := db.Ping(); err != nil {
		panic(err)
	}
	return db
}
