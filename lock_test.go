package barn

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/bibenga/barn-go/internal/adapter"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
	"github.com/stretchr/testify/require"

	_ "github.com/mattn/go-sqlite3"
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

func newSqliteConnection() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
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
	// db, err := newSqliteConnection()
	assert.NoError(err)
	assert.NotNil(db)
	assert.NoError(db.Ping())

	lockQuery := adapter.NewDefaultLockQuery()
	_, err = db.Exec(lockQuery.GetCreateQuery())
	assert.NoError(err)

	entryQuery := adapter.NewDefaultEntryQuery()
	_, err = db.Exec(entryQuery.GetCreateQuery())
	assert.NoError(err)

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

func TestLockAcquire(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	_, err := db.Exec(`insert into barn_lock (name) values ('barn')`)
	assert.NoError(err)
	_, err = db.Exec(`insert into barn_lock (name) values ('unnecessary')`)
	assert.NoError(err)

	manager := NewLockManager(db, &DummyLockListener{})
	captured, err := manager.tryLock()
	assert.NoError(err)
	assert.True(captured)
	assert.True(manager.isLocked)
	assert.NotNil(manager.lockedAt)

	row := db.QueryRow("select locked_at, locked_by from barn_lock where name='barn'")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var locked_at *time.Time
	var locked_by *string
	assert.NoError(row.Scan(&locked_at, &locked_by))
	assert.NotNil(locked_at)
	assert.Equal(manager.lockedAt.Truncate(time.Millisecond), locked_at.In(time.UTC).Truncate(time.Millisecond))
	assert.NotNil(locked_by)
	assert.Equal(manager.hostname, *locked_by)
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
