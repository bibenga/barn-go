package task

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type PostgresQueueRepository struct {
	config TaskQueryConfig
}

func NewPostgresQueueRepository(config ...TaskQueryConfig) TaskRepository {
	var c *TaskQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c = &TaskQueryConfig{}
	}
	r := &PostgresQueueRepository{
		config: *c,
	}
	r.setupDefaults()
	return r
}

func (r *PostgresQueueRepository) setupDefaults() {
	c := &r.config
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.CreatedAtField == "" {
		c.CreatedAtField = DefaultCreatedAtField
	}
	if c.FuncField == "" {
		c.FuncField = DefaultFuncField
	}
	if c.ArgsField == "" {
		c.ArgsField = DefaultArgsField
	}
	if c.IsProcessedField == "" {
		c.IsProcessedField = DefaultIsProcessedField
	}
	if c.StartedAtField == "" {
		c.StartedAtField = DefaultStartedAtField
	}
	if c.FinishedAtField == "" {
		c.FinishedAtField = DefaultFinishedAtField
	}
	if c.IsSuccessField == "" {
		c.IsSuccessField = DefaultIsSuccessField
	}
	if c.ResultField == "" {
		c.ResultField = DefaultResultField
	}
	if c.ErrorField == "" {
		c.ErrorField = DefaultErrorField
	}
}

func (r *PostgresQueueRepository) CreateTable(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s timestamp with time zone not null, 
				%s varchar not null, 
				%s jsonb not null, 
				%s boolean default false not null, 
				%s timestamp with time zone, 
				%s timestamp with time zone, 
				%s boolean, 
				%s varchar, 
				%s varchar, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.CreatedAtField,
			c.FuncField,
			c.ArgsField,
			c.IsProcessedField,
			c.StartedAtField,
			c.FinishedAtField,
			c.IsSuccessField,
			c.ErrorField,
			c.ResultField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		fmt.Sprintf(
			`create index if not exists idx_%s_%s on %s (%s)`,
			strings.ReplaceAll(c.TableName, ".", "_"), c.CreatedAtField, c.TableName, c.CreatedAtField,
		),
	)
	return err
}

func (r *PostgresQueueRepository) FindNext(tx *sql.Tx) (*Task, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
			from %s
			where not %s
			order by %s
			limit 1
			for update skip locked`,
			c.IdField, c.CreatedAtField, c.FuncField, c.ArgsField, c.IsProcessedField, c.StartedAtField, c.FinishedAtField, c.IsSuccessField, c.ResultField, c.ErrorField,
			c.TableName,
			c.IsProcessedField,
			c.CreatedAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var t Task
	row := stmt.QueryRow()
	if err := row.Scan(&t.Id, &t.CreatedAt, &t.Func, &t.Args, &t.IsProcessed, &t.StartedAt, &t.FinishedAt, &t.IsSuccess, &t.Result, &t.Error); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &t, nil
}

func (r *PostgresQueueRepository) Create(tx *sql.Tx, m *Task) error {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9) 
			returning %s`,
			c.TableName,
			c.CreatedAtField, c.FuncField, c.ArgsField, c.IsProcessedField, c.StartedAtField, c.FinishedAtField, c.IsSuccessField, c.ResultField, c.ErrorField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(&m.CreatedAt, &m.Func, &m.Args, &m.IsProcessed, &m.StartedAt, &m.FinishedAt, &m.IsSuccess, &m.Result, &m.Error).Scan(&m.Id)
	return err
}

func (r *PostgresQueueRepository) Save(tx *sql.Tx, t *Task) error {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3, %s=$4, %s=$5, %s=$6
			where %s=$7`,
			c.TableName,
			c.IsProcessedField, c.StartedAtField, c.FinishedAtField, c.IsSuccessField, c.ResultField, c.ErrorField,
			c.IdField,
		),
		t.IsProcessed, t.StartedAt, t.FinishedAt, t.IsSuccess, t.Result, t.Error,
		t.Id,
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

func (r *PostgresQueueRepository) DeleteOld(tx *sql.Tx, moment time.Time) (int, error) {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s and %s<=$1`,
			c.TableName,
			c.IsProcessedField, c.CreatedAtField,
		),
		moment,
	)
	if err != nil {
		return 0, err
	}
	if rowsAffected, err := res.RowsAffected(); err != nil {
		return 0, err
	} else {
		return int(rowsAffected), nil
	}
}

func (r *PostgresQueueRepository) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ TaskRepository = &PostgresQueueRepository{}
