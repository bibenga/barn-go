package lock

import (
	"database/sql"
	"fmt"
	"log/slog"
	"testing"
	"time"

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

	lockQuery := NewDefaultLockQuery()
	_, err = db.Exec(lockQuery.GetCreateTableQuery())
	assert.NoError(err)

	t.Cleanup(func() {
		t.Helper()
		t.Log("db - Close")
		err = db.Close()
		assert.NoError(err)
	})
	return db
}

func TestTryLock(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	_, err := db.Exec(`insert into barn_lock (name) values ('barn')`)
	assert.NoError(err)
	_, err = db.Exec(`insert into barn_lock (name) values ('unnecessary')`)
	assert.NoError(err)

	l := NewLock(db, "barn", 10*time.Second)
	captured, err := l.TryLock()
	assert.NoError(err)
	assert.True(captured)
	assert.True(l.locked)
	assert.NotNil(l.lockedAt)

	row := db.QueryRow("select locked_at, owner from barn_lock where name='barn'")
	assert.NoError(row.Err())
	assert.NotNil(row)
	var locked_at *time.Time
	var owner *string
	assert.NoError(row.Scan(&locked_at, &owner))
	assert.NotNil(locked_at)
	assert.Equal(l.lockedAt.Truncate(time.Millisecond), locked_at.In(time.UTC).Truncate(time.Millisecond))
	assert.NotNil(owner)
	assert.Equal(l.name, *owner)
}

func TestLogState(t *testing.T) {
	assert := require.New(t)

	db := setup(t)

	l := NewLock(db, "barn", 10*time.Second)
	state, err := l.State()
	assert.NoError(err)
	assert.NotNil(state)
}
