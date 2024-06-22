package lock

import "fmt"

const DefaultTableName string = "barn_lock"
const DefaultNameField string = "name"
const DefaultLockedAtField string = "locked_at"
const DefaultOwnerField string = "owner"

type LockQueryConfig struct {
	TableName     string
	NameField     string
	LockedAtField string
	OwnerField    string
}

type LockQuery struct {
	CreateTableQuery string
	InsertQuery      string
	SelectQuery      string
	LockQuery        string
	ConfirmQuery     string
	UnlockQuery      string
}

func NewLockQuery(c LockQueryConfig) LockQuery {
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.NameField == "" {
		c.NameField = DefaultNameField
	}
	if c.LockedAtField == "" {
		c.LockedAtField = DefaultLockedAtField
	}
	if c.OwnerField == "" {
		c.OwnerField = DefaultOwnerField
	}
	return LockQuery{
		CreateTableQuery: fmt.Sprintf(
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
		InsertQuery: fmt.Sprintf(
			`insert into %s(%s) 
			values ($1) 
			on conflict (%s) do nothing`,
			c.TableName, c.NameField,
			c.NameField,
		),
		SelectQuery: fmt.Sprintf(
			`select %s, %s 
			from %s 
			where %s = $1`,
			c.LockedAtField, c.OwnerField,
			c.TableName,
			c.NameField,
		),
		LockQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and (%s is null or %s < $4)`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField, c.LockedAtField, c.LockedAtField,
		),
		ConfirmQuery: fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3 and %s = $4 and %s > $5`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField, c.OwnerField, c.LockedAtField,
		),
		UnlockQuery: fmt.Sprintf(
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
	return NewLockQuery(LockQueryConfig{})
}
