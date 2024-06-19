package barn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
	"github.com/stretchr/testify/require"
)

const driver = "pgx"
const dbname = "barn"
const dbname_test = "barn_test"
const dsn_tpl = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=%s TimeZone=UTC sslmode=disable"

var dsn = fmt.Sprintf(dsn_tpl, dbname)
var dsn_test = fmt.Sprintf(dsn_tpl, dbname_test)
var drop_db_query = fmt.Sprintf(`drop database if exists %s`, dbname_test)
var create_db_query = fmt.Sprintf(`create database %s`, dbname_test)

func newConnection(dsn string) (*sql.DB, error) {
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
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)
	return db, nil
}

func newTestConnection() (*sql.DB, error) {
	db, err := newConnection(dsn)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(drop_db_query); err != nil {
		return nil, err
	}
	if _, err := db.Exec(create_db_query); err != nil {
		return nil, err
	}
	db, err = newConnection(dsn_test)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func setup(t *testing.T) *sql.DB {
	t.Helper()
	assert := require.New(t)

	db, err := newTestConnection()
	assert.NoError(err)
	assert.NotNil(db)
	assert.NoError(db.Ping())

	t.Cleanup(func() {
		t.Helper()
		t.Log("db - Close")
		err = db.Close()
		assert.NoError(err)
	})
	return db
}

func newTx(t *testing.T, readOnly bool) *sql.Tx {
	t.Helper()
	assert := require.New(t)

	t.Log("db - Open")
	db := setup(t)

	t.Log("tx - Begin")
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{
		Isolation: sql.LevelDefault,
		ReadOnly:  readOnly,
	})
	assert.NoError(err)

	t.Cleanup(func() {
		t.Helper()
		t.Log("tx - Rollback")
		err := tx.Rollback()
		assert.NoError(err)
	})
	return tx
}

func TestDb(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	row := db.QueryRow("select 1,2")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var v1, v2 int
	assert.NoError(row.Scan(&v1, &v2))
	assert.Equal(v1, 1)
	assert.Equal(v2, 2)
}

func TestTx(t *testing.T) {
	assert := require.New(t)

	tx := newTx(t, false)
	assert.NotNil(tx)

	row := tx.QueryRow("select 1,2")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var v1, v2 int
	assert.NoError(row.Scan(&v1, &v2))
	assert.Equal(v1, 1)
	assert.Equal(v2, 2)
}
