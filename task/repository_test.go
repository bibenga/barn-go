package task

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaskModelMetaPointer(t *testing.T) {
	assert := require.New(t)

	task := new(Task)
	meta := TaskModelMeta(task)
	assert.Equal(meta.TableName, "barn_task")
}

func TestTaskModelMetaStruct(t *testing.T) {
	assert := require.New(t)

	task := Task{}
	meta := TaskModelMeta(task)
	assert.Equal(meta.TableName, "barn_task")
}
