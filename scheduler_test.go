package barngo

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSchedulerCreate(t *testing.T) {
	assert := require.New(t)

	db := setupTestDb(t)

	scheduler := NewScheduler[Schedule](db)

	err := RunInTransaction(db, func(tx *sql.Tx) error {
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
