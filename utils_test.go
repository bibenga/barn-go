package barngo

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestModel struct {
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

	task := new(TestModel)
	meta := GetTableMeta(task)
	assert.Equal(meta.TableName, "test_model")
}

func TestTaskModelMetaStruct(t *testing.T) {
	assert := require.New(t)

	task := TestModel{}
	meta := GetTableMeta(task)
	assert.Equal(meta.TableName, "test_model")
}
