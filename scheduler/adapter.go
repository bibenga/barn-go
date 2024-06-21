package scheduler

import "fmt"

type EntryQueryConfig struct {
	tableName     string
	idField       string
	nameField     string
	isActiveField string
	cronField     string
	nextTsField   string
	lastTsField   string
	messageField  string
}

var defaultEntryQueryConfig = EntryQueryConfig{
	tableName:     "barn_entry",
	idField:       "id",
	nameField:     "name",
	isActiveField: "is_active",
	cronField:     "cron",
	nextTsField:   "next_ts",
	lastTsField:   "last_ts",
	messageField:  "message",
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
			c.tableName,
			c.idField,
			c.nameField,
			c.isActiveField,
			c.cronField,
			c.nextTsField,
			c.lastTsField,
			c.messageField,
			c.idField,
			c.nameField,
		),
		selectAllQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s`,
			c.idField, c.nameField, c.isActiveField, c.cronField, c.nextTsField, c.lastTsField, c.messageField,
			c.tableName,
		),
		selectActiveQuery: fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s`,
			c.idField, c.nameField, c.isActiveField, c.cronField, c.nextTsField, c.lastTsField, c.messageField,
			c.tableName,
			c.isActiveField,
		),
		insertQuery: fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s) 
			values ($1, $2, $3, $4) 
			returning %s`,
			c.tableName,
			c.nameField, c.cronField, c.nextTsField, c.messageField,
			c.idField,
		),
		deleteQuery: fmt.Sprintf(
			`delete from %s 
			where %s=$1`,
			c.tableName,
			c.idField,
		),
		deleteAllQuery: fmt.Sprintf(
			`delete from %s`,
			c.tableName,
		),
		updateQuery: fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3
			where %s=$4`,
			c.tableName,
			c.isActiveField, c.nextTsField, c.lastTsField,
			c.idField,
		),
		updateIsActiveQuery: fmt.Sprintf(
			`update %s 
			set %s=$1
			where %s=$2`,
			c.tableName,
			c.isActiveField,
			c.idField,
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
