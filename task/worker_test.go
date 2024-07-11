package task

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

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

	worker := NewWorker2[Task](db)
	err = barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		if err := worker.CreateTable(tx); err != nil {
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

func TestWorkerCreate(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	worker := NewWorker2[Task](db)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		t := Task{
			Func: "sentEmail",
			Args: map[string]any{"str": "str", "int": 12},
		}
		if err := worker.Create(tx, &t); err != nil {
			return err
		}
		assert.Greater(t.Id, 0)
		return nil
	})
	assert.NoError(err)

	row := db.QueryRow("select count(*) from barn_task")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var count int
	assert.NoError(row.Scan(&count))
	assert.Equal(count, 1)
}

func TestWorkerFindNext(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	worker := NewWorker2[Task](db)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		t := Task{
			Func: "sentEmail",
			Args: map[string]any{"str": "str", "int": 12},
		}
		if err := worker.Create(tx, &t); err != nil {
			return err
		}

		if f, err := worker.FindNext(tx); err != nil {
			return err
		} else {
			assert.NotNil(f)
			assert.Equal(f.Id, t.Id)
		}
		return nil
	})
	assert.NoError(err)

	row := db.QueryRow("select count(*) from barn_task")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var count int
	assert.NoError(row.Scan(&count))
	assert.Equal(count, 1)
}

func TestWorkerFindNextPending(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	worker := NewWorker2[Task](db)

	err := barngo.RunInTransaction(db, func(tx *sql.Tx) error {
		t := Task{
			RunAt: time.Now().UTC().Add(1 * time.Hour),
			Func:  "sentEmail",
			Args:  map[string]any{"str": "str", "int": 12},
		}
		if err := worker.Create(tx, &t); err != nil {
			return err
		}

		if f, err := worker.FindNext(tx); err != nil {
			return err
		} else {
			assert.Nil(f)
		}

		return nil
	})
	assert.NoError(err)

	row := db.QueryRow("select count(*) from barn_task")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var count int
	assert.NoError(row.Scan(&count))
	assert.Equal(count, 1)
}
