package barngo

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jackc/pgx/v5/tracelog"
	pgxslog "github.com/mcosta74/pgx-slog"
	"github.com/stretchr/testify/require"
)

func newTestDb(t *testing.T) *sql.DB {
	t.Helper()
	assert := require.New(t)

	const driverName = "pgx"
	const mainName = "barn"
	const testDbName = "barn_test"
	const dsnTemplate = "host=host.docker.internal port=5432 user=rds password=sqlsql dbname=%s TimeZone=UTC sslmode=disable"

	var mainDsn = fmt.Sprintf(dsnTemplate, mainName)
	var testDsn = fmt.Sprintf(dsnTemplate, testDbName)
	var dropTestDbQuery = fmt.Sprintf(`drop database if exists %s`, testDbName)
	var createTestDbQuery = fmt.Sprintf(`create database %s`, testDbName)

	newDb := func(dsn string) *sql.DB {
		t.Helper()
		connConfig, err := pgx.ParseConfig(dsn)
		assert.NoError(err)

		connConfig.Tracer = &tracelog.TraceLog{
			Logger:   pgxslog.NewLogger(slog.Default()),
			LogLevel: tracelog.LogLevelDebug,
		}
		connectionString := stdlib.RegisterConnConfig(connConfig)
		db, err := sql.Open(driverName, connectionString)
		assert.NoError(err)

		return db
	}

	// connect to themain db
	db := newDb(mainDsn)
	defer db.Close()
	assert.NotNil(db)
	assert.NoError(db.Ping())

	_, err := db.Exec(dropTestDbQuery)
	assert.NoError(err)
	_, err = db.Exec(createTestDbQuery)
	assert.NoError(err)
	err = db.Close()
	assert.NoError(err)

	// connect to the test db
	db = newDb(testDsn)
	assert.NotNil(db)
	assert.NoError(db.Ping())

	// test database schema
	bytes, err := os.ReadFile("schema_test.sql")
	if err != nil {
		panic(err)
	}
	schema := string(bytes)
	err = RunInTransaction(db, func(tx *sql.Tx) error {
		_, err := tx.Exec(schema)
		return err
	})
	assert.NoError(err)

	t.Cleanup(func() {
		t.Helper()
		err = db.Close()
		assert.NoError(err)
	})
	return db
}

type testModel struct {
	Id         int        `barn:""`
	RunAt      time.Time  `barn:""`
	Func       string     `barn:""`
	Args       any        `barn:""`
	Status     string     `barn:""`
	StartedAt  *time.Time `barn:""`
	FinishedAt *time.Time `barn:""`
	Result     any        `barn:""`
	Error      *string    `barn:""`
}

func TestTaskModelMetaPointer(t *testing.T) {
	assert := require.New(t)

	task := new(testModel)
	meta := GetTableMeta(task)
	assert.Equal(meta.TableName, "test_model")
}
