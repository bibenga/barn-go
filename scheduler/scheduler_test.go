package scheduler

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	barngo "github.com/bibenga/barn-go"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
	"github.com/stretchr/testify/require"
)

const driver = "pgx"
const dbName = "barn"
const testDbName = "barn_test"
const dsn_tpl = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=%s TimeZone=UTC sslmode=disable"

var dsn = fmt.Sprintf(dsn_tpl, dbName)
var testDsn = fmt.Sprintf(dsn_tpl, testDbName)
var dropDbQuery = fmt.Sprintf(`drop database if exists %s`, testDbName)
var createDbQuery = fmt.Sprintf(`create database %s`, testDbName)

func newDb(dsn string) (*sql.DB, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	connConfig.Tracer = &tracelog.TraceLog{
		Logger:   pgxslog.NewLogger(slog.Default()),
		LogLevel: tracelog.LogLevelDebug,
	}
	connectionString := stdlib.RegisterConnConfig(connConfig)
	db, err := sql.Open(driver, connectionString)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func newTestDb() (*sql.DB, error) {
	// connect to some db
	db, err := newDb(dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(dropDbQuery); err != nil {
		return nil, err
	}
	if _, err := db.Exec(createDbQuery); err != nil {
		return nil, err
	}
	// connect to test db
	db, err = newDb(testDsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func setup(t *testing.T) *sql.DB {
	t.Helper()
	assert := require.New(t)

	db, err := newTestDb()
	assert.NoError(err)
	assert.NotNil(db)
	assert.NoError(db.Ping())

	scheduler := NewSimpleScheduler2[Schedule](db)
	err = barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		if err := scheduler.CreateTable(tx); err != nil {
			return err
		}
		return nil
	})
	assert.NoError(err)

	t.Cleanup(func() {
		t.Helper()
		t.Log("db - Close")
		err = db.Close()
		assert.NoError(err)
	})
	return db
}

func TestSchedulerCreate(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	scheduler := NewSimpleScheduler2[Schedule](db)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		t := Schedule{
			Func: "sentEmail",
			Args: map[string]any{"str": "str", "int": 12},
		}
		if err := scheduler.Create(tx, &t); err != nil {
			return err
		}
		assert.Greater(t.Id, 0)
		return nil
	})
	assert.NoError(err)

	row := db.QueryRow("select count(*) from barn_schedule")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var count int
	assert.NoError(row.Scan(&count))
	assert.Equal(count, 1)
}
