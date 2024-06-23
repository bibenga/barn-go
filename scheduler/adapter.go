package scheduler

import "fmt"

const DefaultTableName = "barn_task"
const DefaultIdField = "id"
const DefaultNameField = "name"
const DefaultIsActiveField = "is_active"
const DefaultCronField = "cron"
const DefaultNextTsField = "next_ts"
const DefaultLastTsField = "last_ts"
const DefaultMessageField = "message"

type TaskQueryConfig struct {
	TableName     string
	IdField       string
	NameField     string
	IsActiveField string
	CronField     string
	NextTsField   string
	LastTsField   string
	MessageField  string
}

func (c *TaskQueryConfig) init() {
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.NameField == "" {
		c.NameField = DefaultNameField
	}
	if c.IsActiveField == "" {
		c.IsActiveField = DefaultIsActiveField
	}
	if c.CronField == "" {
		c.CronField = DefaultCronField
	}
	if c.NextTsField == "" {
		c.NextTsField = DefaultNextTsField
	}
	if c.LastTsField == "" {
		c.LastTsField = DefaultLastTsField
	}
	if c.MessageField == "" {
		c.MessageField = DefaultMessageField
	}
}

type TaskQuery struct {
	CreateTableQuery    string
	SelectQuery         string
	InsertQuery         string
	DeleteQuery         string
	DeleteAllQuery      string
	UpdateQuery         string
	UpdateIsActiveQuery string
}

func NewTaskQuery(c *TaskQueryConfig) TaskQuery {
	c.init()
	return TaskQuery{
		CreateTableQuery: fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				%s SERIAL NOT NULL, 
				%s VARCHAR NOT NULL, 
				%s BOOLEAN DEFAULT TRUE NOT NULL, 
				%s VARCHAR, 
				%s TIMESTAMP WITH TIME ZONE, 
				%s TIMESTAMP WITH TIME ZONE, 
				%s JSONB, 
				PRIMARY KEY (%s),
				UNIQUE (%s)
			)`,
			c.TableName,
			c.IdField,
			c.NameField,
			c.IsActiveField,
			c.CronField,
			c.NextTsField,
			c.LastTsField,
			c.MessageField,
			c.IdField,
			c.NameField,
		),
		SelectQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
		),
		InsertQuery: fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s) 
			values ($1, $2, $3, $4) 
			returning %s`,
			c.TableName,
			c.NameField, c.CronField, c.NextTsField, c.MessageField,
			c.IdField,
		),
		DeleteQuery: fmt.Sprintf(
			`delete from %s 
			where %s=$1`,
			c.TableName,
			c.IdField,
		),
		DeleteAllQuery: fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
		UpdateQuery: fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3
			where %s=$4`,
			c.TableName,
			c.IsActiveField, c.NextTsField, c.LastTsField,
			c.IdField,
		),
		UpdateIsActiveQuery: fmt.Sprintf(
			`update %s 
			set %s=$1
			where %s=$2`,
			c.TableName,
			c.IsActiveField,
			c.IdField,
		),
	}
}

func NewDefaultTaskQuery() TaskQuery {
	return NewTaskQuery(&TaskQueryConfig{})
}

type SimpleTaskQuery struct {
	CreateTableQuery      string
	SelectForInitQuery    string
	SelectForProcessQuery string
	UpdateQuery           string
	UpdateIsActiveQuery   string
}

func NewSimpleTaskQuery(c *TaskQueryConfig) SimpleTaskQuery {
	q := NewTaskQuery(c)
	return SimpleTaskQuery{
		CreateTableQuery: q.CreateTableQuery,
		SelectForInitQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s and %s is null
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField, c.NextTsField,
		),
		SelectForProcessQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s and %s < current_timestamp
			limit $1
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField, c.NextTsField,
		),
		UpdateQuery:         q.UpdateQuery,
		UpdateIsActiveQuery: q.UpdateIsActiveQuery,
	}
}

func NewDefaultSimpleTaskQuery() SimpleTaskQuery {
	return NewSimpleTaskQuery(&TaskQueryConfig{})
}
