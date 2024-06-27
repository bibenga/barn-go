package lock

import (
	"database/sql"
	"fmt"
)

type PostgresLockRepository struct {
	config LockQueryConfig
}

func NewPostgresLockRepository(config ...LockQueryConfig) LockRepository {
	var c *LockQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c = &LockQueryConfig{}
	}
	r := &PostgresLockRepository{
		config: *c,
	}
	r.setupDefaults()
	return r
}

func (r *PostgresLockRepository) setupDefaults() {
	c := &r.config
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

func (r *PostgresLockRepository) CreateTable(tx *sql.Tx) error {
	c := &r.config
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
	c := &r.config
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
	c := &r.config
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
	c := &r.config
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

func (r *PostgresLockRepository) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ LockRepository = &PostgresLockRepository{}
