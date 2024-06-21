package lock

import "fmt"

type LockQueryConfig struct {
	TableName     string
	NameField     string
	LockedAtField string
	OwnerField    string
}

var defaultLockQueryConfig = LockQueryConfig{
	TableName:     "barn_lock",
	NameField:     "name",
	LockedAtField: "locked_at",
	OwnerField:    "owner",
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
			c.TableName,
			c.NameField,
			c.LockedAtField,
			c.OwnerField,
			c.NameField,
		),
		insertQuery: fmt.Sprintf(
			`insert into %s(%s) 
			values ($1) 
			on conflict (%s) do nothing`,
			c.TableName, c.NameField,
			c.NameField,
		),
		selectQuery: fmt.Sprintf(
			`select %s, %s 
			from %s 
			where %s = $1`,
			c.LockedAtField, c.OwnerField,
			c.TableName,
			c.NameField,
		),
		lockQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and (%s is null or %s < $4)`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField, c.LockedAtField, c.LockedAtField,
		),
		confirmQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and %s = $4 and %s > $5`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField, c.OwnerField, c.LockedAtField,
		),
		unlockQuery: fmt.Sprintf(
			`update %s 
			set %s = null, %s = null
			where %s = $1 and %s = $2 and %s > $3`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField, c.OwnerField, c.LockedAtField,
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
