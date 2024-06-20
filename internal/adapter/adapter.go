package adapter

import "fmt"

type LockQuery struct {
	tableName     string
	nameField     string
	lockedAtField string
	lockedByField string
}

func NewDefaultLockQuery() LockQuery {
	return LockQuery{
		tableName:     "barn_lock",
		nameField:     "name",
		lockedAtField: "locked_at",
		lockedByField: "locked_by",
	}
}

func (q *LockQuery) GetCreateQuery() string {
	return fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s  (
			%s VARCHAR NOT NULL,
			%s TIMESTAMP WITH TIME ZONE,
			%s VARCHAR,
			PRIMARY KEY (%s)
		)`,
		q.tableName,
		q.nameField,
		q.lockedAtField,
		q.lockedByField,
		q.nameField,
	)
}

func (q *LockQuery) GetIsExistQuery() string {
	return fmt.Sprintf(
		`select 1 from %s where name = $1 limit 1`,
		q.tableName,
	)
}

func (q *LockQuery) GetInsertQuery() string {
	return fmt.Sprintf(
		`insert into %s(%s) 
		values ($1) 
		on conflict (%s) do nothing`,
		q.tableName, q.nameField,
		q.nameField,
	)
}

func (q *LockQuery) GetSelectQuery() string {
	return fmt.Sprintf(
		`select %s, %s 
		from %s 
		where %s = $1`,
		q.lockedAtField, q.lockedByField,
		q.tableName,
		q.nameField,
	)
}

func (q *LockQuery) GetLockQuery() string {
	return fmt.Sprintf(
		`update %s 
		set %s = $1, %s = $2 
		where %s = $3 and (%s is null or %s < $4)`,
		q.tableName,
		q.lockedByField, q.lockedAtField,
		q.nameField, q.lockedAtField, q.lockedAtField,
	)
}

func (q *LockQuery) GetConfirmQuery() string {
	return fmt.Sprintf(
		`update %s 
		set %s = $1, %s = $2 
		where %s = $3 and %s = $4 and %s > $5`,
		q.tableName,
		q.lockedByField, q.lockedAtField,
		q.nameField, q.lockedByField, q.lockedAtField,
	)
}

func (q *LockQuery) GetUnlockQuery() string {
	return fmt.Sprintf(
		`update %s 
		set %s = null, %s = null
		where %s = $1 and %s = $2 and (%s is null or %s > $3)`,
		q.tableName,
		q.lockedByField, q.lockedAtField,
		q.nameField, q.lockedByField, q.lockedAtField, q.lockedAtField,
	)
}

type EntryQuery struct {
	tableName     string
	idField       string
	nameField     string
	isActiveField string
	cronField     string
	nextTsField   string
	lastTsField   string
	messageField  string
}

func NewDefaultEntryQuery() EntryQuery {
	return EntryQuery{
		tableName:     "barn_entry",
		idField:       "id",
		nameField:     "name",
		isActiveField: "is_active",
		cronField:     "cron",
		nextTsField:   "next_ts",
		lastTsField:   "last_ts",
		messageField:  "message",
	}
}

func (q *EntryQuery) GetCreateQuery() string {
	return fmt.Sprintf(
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
		q.tableName,
		q.idField,
		q.nameField,
		q.isActiveField,
		q.cronField,
		q.nextTsField,
		q.lastTsField,
		q.messageField,
		q.idField,
		q.nameField,
	)
}

func (q *EntryQuery) GetSelectAllQuery() string {
	return fmt.Sprintf(
		`select %s, %s, %s, %s, %s, %s, %s 
		from %s`,
		q.idField, q.nameField, q.isActiveField, q.cronField, q.nextTsField, q.lastTsField, q.messageField,
		q.tableName,
	)
}

func (q *EntryQuery) GetSelectActiveQuery() string {
	return fmt.Sprintf(
		`select %s, %s, %s, %s, %s, %s, %s 
		from %s
		where %s`,
		q.idField, q.nameField, q.isActiveField, q.cronField, q.nextTsField, q.lastTsField, q.messageField,
		q.tableName,
		q.isActiveField,
	)
}

func (q *EntryQuery) GetInsertQuery() string {
	return fmt.Sprintf(
		`insert into %s(%s, %s, %s, %s) 
		values ($1, $2, $3, $4) 
		returning %s`,
		q.tableName,
		q.nameField, q.cronField, q.nextTsField, q.messageField,
		q.idField,
	)
}

func (q *EntryQuery) GetDeleteQuery() string {
	return fmt.Sprintf(
		`delete from %s 
		where %s=$1`,
		q.tableName,
		q.idField,
	)
}

func (q *EntryQuery) GetDeleteAllQuery() string {
	return fmt.Sprintf(
		`delete from %s`,
		q.tableName,
	)
}

func (q *EntryQuery) GetUpdateQuery() string {
	return fmt.Sprintf(
		`update %s 
		set %s=$1, %s=$2, %s=$3
		where %s=$4`,
		q.tableName,
		q.isActiveField, q.nextTsField, q.lastTsField,
		q.idField,
	)
}

func (q *EntryQuery) GetUpdateIsActiveQuery() string {
	return fmt.Sprintf(
		`update %s 
		set %s=$1
		where %s=$2`,
		q.tableName,
		q.isActiveField,
		q.idField,
	)
}
