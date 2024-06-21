package lock

import "fmt"

type LockQueryConfig struct {
	tableName     string
	nameField     string
	lockedAtField string
	ownerField    string
}

var defaultLockQueryConfig = LockQueryConfig{
	tableName:     "barn_lock",
	nameField:     "name",
	lockedAtField: "locked_at",
	ownerField:    "owner",
}

type LockQuery struct {
	createTableQuery string
	insertQuery      string
	selectQuery      string
	lockQuery        string
	confirmQuery     string
	unlockQuery      string
}

func NewLockQuery(c LockQueryConfig) LockQuery {
	return LockQuery{
		createTableQuery: fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s  (
				%s VARCHAR NOT NULL,
				%s TIMESTAMP WITH TIME ZONE,
				%s VARCHAR,
				PRIMARY KEY (%s)
			)`,
			c.tableName,
			c.nameField,
			c.lockedAtField,
			c.ownerField,
			c.nameField,
		),
		insertQuery: fmt.Sprintf(
			`insert into %s(%s) 
			values ($1) 
			on conflict (%s) do nothing`,
			c.tableName, c.nameField,
			c.nameField,
		),
		selectQuery: fmt.Sprintf(
			`select %s, %s 
			from %s 
			where %s = $1`,
			c.lockedAtField, c.ownerField,
			c.tableName,
			c.nameField,
		),
		lockQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and (%s is null or %s < $4)`,
			c.tableName,
			c.ownerField, c.lockedAtField,
			c.nameField, c.lockedAtField, c.lockedAtField,
		),
		confirmQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and %s = $4 and %s > $5`,
			c.tableName,
			c.ownerField, c.lockedAtField,
			c.nameField, c.ownerField, c.lockedAtField,
		),
		unlockQuery: fmt.Sprintf(
			`update %s 
			set %s = null, %s = null
			where %s = $1 and %s = $2 and %s > $3`,
			c.tableName,
			c.ownerField, c.lockedAtField,
			c.nameField, c.ownerField, c.lockedAtField, c.lockedAtField,
		),
	}
}

func NewDefaultLockQuery() LockQuery {
	return NewLockQuery(defaultLockQueryConfig)
}

func (q *LockQuery) GetCreateTableQuery() string {
	return q.createTableQuery
}

func (q *LockQuery) GetInsertQuery() string {
	return q.insertQuery
}

func (q *LockQuery) GetSelectQuery() string {
	return q.selectQuery
}

func (q *LockQuery) GetLockQuery() string {
	return q.lockQuery
}

func (q *LockQuery) GetConfirmQuery() string {
	return q.confirmQuery
}

func (q *LockQuery) GetUnlockQuery() string {
	return q.unlockQuery
}
