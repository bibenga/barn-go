package task

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type PostgresTaskRepository struct {
	config TaskQueryConfig
}

func NewPostgresTaskRepository(config ...TaskQueryConfig) TaskRepository {
	var c *TaskQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c1 := TaskModelMeta(new(Task))
		c = &c1
		// c = &TaskQueryConfig{}
	}
	r := &PostgresTaskRepository{
		config: *c,
	}
	r.setupDefaults()
	return r
}

func (r *PostgresTaskRepository) setupDefaults() {
	c := &r.config
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.RunAtField == "" {
		c.RunAtField = DefaultRunAtField
	}
	if c.FuncField == "" {
		c.FuncField = DefaultFuncField
	}
	if c.ArgsField == "" {
		c.ArgsField = DefaultArgsField
	}
	if c.StatusField == "" {
		c.StatusField = DefaultStatusField
	}
	if c.StartedAtField == "" {
		c.StartedAtField = DefaultStartedAtField
	}
	if c.FinishedAtField == "" {
		c.FinishedAtField = DefaultFinishedAtField
	}
	if c.ResultField == "" {
		c.ResultField = DefaultResultField
	}
	if c.ErrorField == "" {
		c.ErrorField = DefaultErrorField
	}
}

func (r *PostgresTaskRepository) CreateTable(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s timestamp with time zone not null, 
				%s varchar not null, 
				%s jsonb, 
				%s char(1) default 'Q' not null, 
				%s timestamp with time zone, 
				%s timestamp with time zone, 
				%s jsonb, 
				%s varchar, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.RunAtField,
			c.FuncField,
			c.ArgsField,
			c.StatusField,
			c.StartedAtField,
			c.FinishedAtField,
			c.ResultField,
			c.ErrorField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}

	_, err = tx.Exec(
		fmt.Sprintf(
			`create index if not exists idx_%s_%s on %s (%s)`,
			strings.ReplaceAll(c.TableName, ".", "_"), c.RunAtField, c.TableName, c.RunAtField,
		),
	)
	return err
}

func (r *PostgresTaskRepository) FindNext(tx *sql.Tx) (*Task, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s, %s
			from %s
			where %s = $1 and %s < $2
			order by %s
			limit 1
			for update skip locked`,
			c.IdField, c.RunAtField, c.FuncField, c.ArgsField, c.StatusField, c.StartedAtField, c.FinishedAtField, c.ResultField, c.ErrorField,
			c.TableName,
			c.StatusField, c.RunAtField,
			c.RunAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var t Task
	var args []byte
	var result []byte
	row := stmt.QueryRow(Queued, time.Now().UTC())
	if err := row.Scan(&t.Id, &t.RunAt, &t.Func, &args, &t.Status, &t.StartedAt, &t.FinishedAt, &result, &t.Error); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	if args != nil {
		if err := json.Unmarshal(args, &t.Args); err != nil {
			return nil, err
		}
	}
	if result != nil {
		if err := json.Unmarshal(result, &t.Result); err != nil {
			return nil, err
		}
	}
	return &t, nil
}

func (r *PostgresTaskRepository) Create(tx *sql.Tx, t *Task) error {
	c := &r.config
	if t.RunAt.IsZero() {
		t.RunAt = time.Now().UTC()
	}
	if t.Status == "" {
		t.Status = Queued
	}
	args, err := json.Marshal(t.Args)
	if err != nil {
		return err
	}
	result, err := json.Marshal(t.Result)
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5, $6, $7, $8) 
			returning %s`,
			c.TableName,
			c.RunAtField, c.FuncField, c.ArgsField, c.StatusField, c.StartedAtField, c.FinishedAtField, c.ResultField, c.ErrorField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(t.RunAt, t.Func, args, t.Status, t.StartedAt, t.FinishedAt, result, t.Error).Scan(&t.Id)
	return err
}

func (r *PostgresTaskRepository) Save(tx *sql.Tx, t *Task) error {
	c := &r.config
	result, err := json.Marshal(t.Result)
	if err != nil {
		return err
	}
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3, %s=$4, %s=$5
			where %s=$6`,
			c.TableName,
			c.StatusField, c.StartedAtField, c.FinishedAtField, c.ResultField, c.ErrorField,
			c.IdField,
		),
		t.Status, t.StartedAt, t.FinishedAt, result, t.Error,
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

func (r *PostgresTaskRepository) DeleteOld(tx *sql.Tx, moment time.Time) (int, error) {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s in ($1, $2) and %s<=$3`,
			c.TableName,
			c.StatusField, c.RunAtField,
		),
		Done, Failed, moment,
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

func (r *PostgresTaskRepository) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ TaskRepository = &PostgresTaskRepository{}
