package scheduler

import "fmt"

type EntryQueryConfig struct {
	TableName     string
	IdField       string
	NameField     string
	IsActiveField string
	CronField     string
	NextTsField   string
	LastTsField   string
	MessageField  string
}

var defaultEntryQueryConfig = EntryQueryConfig{
	TableName:     "barn_entry",
	IdField:       "id",
	NameField:     "name",
	IsActiveField: "is_active",
	CronField:     "cron",
	NextTsField:   "next_ts",
	LastTsField:   "last_ts",
	MessageField:  "message",
}

type EntryQuery struct {
	createTableQuery    string
	selectAllQuery      string
	selectActiveQuery   string
	insertQuery         string
	deleteQuery         string
	deleteAllQuery      string
	updateQuery         string
	updateIsActiveQuery string
}

func NewEntryQuery(c EntryQueryConfig) EntryQuery {
	return EntryQuery{
		createTableQuery: fmt.Sprintf(
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
		selectAllQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
		),
		selectActiveQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField,
		),
		insertQuery: fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s) 
			values ($1, $2, $3, $4) 
			returning %s`,
			c.TableName,
			c.NameField, c.CronField, c.NextTsField, c.MessageField,
			c.IdField,
		),
		deleteQuery: fmt.Sprintf(
			`delete from %s 
			where %s=$1`,
			c.TableName,
			c.IdField,
		),
		deleteAllQuery: fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
		updateQuery: fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3
			where %s=$4`,
			c.TableName,
			c.IsActiveField, c.NextTsField, c.LastTsField,
			c.IdField,
		),
		updateIsActiveQuery: fmt.Sprintf(
			`update %s 
			set %s=$1
			where %s=$2`,
			c.TableName,
			c.IsActiveField,
			c.IdField,
		),
	}
}

func NewDefaultEntryQuery() EntryQuery {
	return NewEntryQuery(defaultEntryQueryConfig)
}

func (q *EntryQuery) GetCreateTableQuery() string {
	return q.createTableQuery
}

func (q *EntryQuery) GetSelectAllQuery() string {
	return q.selectAllQuery
}

func (q *EntryQuery) GetSelectActiveQuery() string {
	return q.selectActiveQuery
}

func (q *EntryQuery) GetInsertQuery() string {
	return q.insertQuery
}

func (q *EntryQuery) GetDeleteQuery() string {
	return q.deleteQuery
}

func (q *EntryQuery) GetDeleteAllQuery() string {
	return q.deleteAllQuery
}

func (q *EntryQuery) GetUpdateQuery() string {
	return q.updateQuery
}

func (q *EntryQuery) GetUpdateIsActiveQuery() string {
	return q.updateIsActiveQuery
}
