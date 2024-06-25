package lock

import (
	"database/sql"
	"fmt"
	"time"
)

const DefaultTableName = "barn_lock"
const DefaultNameField = "name"
const DefaultLockedAtField = "locked_at"
const DefaultOwnerField = "owner"

type LockQueryConfig struct {
	TableName     string
	NameField     string
	LockedAtField string
	OwnerField    string
}

func (c *LockQueryConfig) init() {
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
}

type Lock struct {
	Name     string
	LockedAt *time.Time
	Owner    *string
}

type LockRepository interface {
	FindOne(tx *sql.Tx, name string) (*Lock, error)
	Create(tx *sql.Tx, name string) error
	Save(tx *sql.Tx, lock *Lock) error
}

type PostgresLockRepository struct {
	Config *LockQueryConfig
}

func NewPostgresLockRepository(conig *LockQueryConfig) LockRepository {
	conig.init()
	return &PostgresLockRepository{
		Config: conig,
	}
}

func NewDefaultPostgresLockRepository() LockRepository {
	return NewPostgresLockRepository(&LockQueryConfig{})
}

func (r *PostgresLockRepository) CreateTable(tx *sql.Tx) error {
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
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
	)
	return err
}

func (r *PostgresLockRepository) FindOne(tx *sql.Tx, name string) (*Lock, error) {
	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s 
			from %s 
			where %s = $1
			for update`,
			c.LockedAtField, c.OwnerField,
			c.TableName,
			c.NameField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var state = Lock{Name: name}
	row := stmt.QueryRow(name)
	if err := row.Scan(&state.LockedAt, &state.Owner); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &state, nil
}

func (r *PostgresLockRepository) Create(tx *sql.Tx, name string) error {
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
			`insert into %s (%s) 
			values ($1) 
			on conflict (%s) do nothing`,
			c.TableName, c.NameField,
			c.NameField,
		),
		name,
	)
	return err
}

func (r *PostgresLockRepository) Save(tx *sql.Tx, lock *Lock) error {
	c := r.Config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s = $1, %s = $2 
			where %s = $3`,
			c.TableName,
			c.OwnerField, c.LockedAtField,
			c.NameField,
		),
		lock.Owner, lock.LockedAt,
		lock.Name,
	)
	if err != nil {
		return err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		return err
	} else {
		if rowsAffected == 0 {
			return sql.ErrNoRows
		}
	}
	return nil
}

var _ LockRepository = &PostgresLockRepository{}
