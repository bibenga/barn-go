package barngo

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerCreate(t *testing.T) {
	assert := require.New(t)

	db := setupTestDb(t)

	worker := NewWorker[Task](db)

	err := RunInTransaction(db, func(tx *sql.Tx) error {
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

	db := setupTestDb(t)

	worker := NewWorker[Task](db)

	err := RunInTransaction(db, func(tx *sql.Tx) error {
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

	db := setupTestDb(t)

	worker := NewWorker[Task](db)

	err := RunInTransaction(db, func(tx *sql.Tx) error {
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
