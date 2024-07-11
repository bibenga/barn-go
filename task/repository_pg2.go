package task

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type PostgresTaskRepository2[T any] struct {
	config TaskQueryConfig
}

func NewPostgresTaskRepository2[T any]() TaskRepository2[T] {
	c := TaskModelMeta(new(Task))
	r := &PostgresTaskRepository2[T]{
		config: c,
	}
	return r
}

func (r *PostgresTaskRepository2[T]) CreateTable(tx *sql.Tx) error {
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
			c.Id.DbName,
			c.RunAt.DbName,
			c.Func.DbName,
			c.Args.DbName,
			c.Status.DbName,
			c.StartedAt.DbName,
			c.FinishedAt.DbName,
			c.Result.DbName,
			c.Error.DbName,
			c.Id.DbName,
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

func (r *PostgresTaskRepository2[T]) FindNext(tx *sql.Tx) (*T, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s, %s
			from %s
			where %s = $1 and %s < $2
			order by %s
			limit 1
			for update skip locked`,
			c.Id.DbName, c.RunAt.DbName,
			c.Func.DbName, c.Args.DbName,
			c.Status.DbName, c.StartedAt.DbName, c.FinishedAt.DbName, c.Result.DbName, c.Error.DbName,
			c.TableName,
			c.Status.DbName, c.RunAt.DbName,
			c.RunAt.DbName,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var id any
	var runAt any
	var tfunc any
	var args any
	var status any
	var startedAt any
	var finishedAt any
	var result any
	var terror any

	// var args []byte
	// var result []byte
	row := stmt.QueryRow(Queued, time.Now().UTC())
	if err := row.Scan(&id, &runAt, &tfunc, &args, &status, &startedAt, &finishedAt, &result, &terror); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	if args != nil {
		bytes := args.([]byte)
		if err := json.Unmarshal(bytes, &args); err != nil {
			return nil, err
		}
	}
	if result != nil {
		bytes := result.([]byte)
		if err := json.Unmarshal(bytes, &result); err != nil {
			return nil, err
		}
	}

	var t = new(T)
	v := reflect.ValueOf(t).Elem()

	SetFieldValue(v.FieldByName(c.Id.Name), id)
	SetFieldValue(v.FieldByName(c.RunAt.Name), runAt)
	SetFieldValue(v.FieldByName(c.Func.Name), tfunc)
	SetFieldValue(v.FieldByName(c.Args.Name), args)
	SetFieldValue(v.FieldByName(c.Status.Name), status)
	SetFieldValue(v.FieldByName(c.StartedAt.Name), startedAt)
	SetFieldValue(v.FieldByName(c.FinishedAt.Name), finishedAt)
	SetFieldValue(v.FieldByName(c.Result.Name), result)
	SetFieldValue(v.FieldByName(c.Error.Name), terror)

	return t, nil
}

func (r *PostgresTaskRepository2[T]) Create(tx *sql.Tx, t *T) error {
	c := &r.config

	v := reflect.ValueOf(t).Elem()

	var runAt any = v.FieldByName(c.RunAt.Name).Interface()
	var tfunc any = v.FieldByName(c.Func.Name).Interface()
	var args any = v.FieldByName(c.Args.Name).Interface()
	var status any = v.FieldByName(c.Status.Name).Interface()
	var startedAt any = v.FieldByName(c.StartedAt.Name).Interface()
	var finishedAt any = v.FieldByName(c.FinishedAt.Name).Interface()
	var result any = v.FieldByName(c.Result.Name).Interface()
	var terror any = v.FieldByName(c.Error.Name).Interface()

	// runAt
	if v, ok := runAt.(time.Time); ok {
		if v.IsZero() {
			runAt = time.Now().UTC()
		}
	} else if v, ok := runAt.(*time.Time); ok {
		if v == nil {
			v1 := time.Now().UTC()
			runAt = &v1
		}
	}

	// status
	if v, ok := status.(Status); ok {
		if v == "" {
			status = Queued
		}
	} else {
		status = Queued
	}

	// args
	args, err := json.Marshal(args)
	if err != nil {
		return err
	}

	// result
	result, err = json.Marshal(result)
	if err != nil {
		return err
	}

	// query
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5, $6, $7, $8) 
			returning %s`,
			c.TableName,
			c.RunAt.DbName, c.Func.DbName, c.Args.DbName,
			c.Status.DbName, c.StartedAt.DbName, c.FinishedAt.DbName,
			c.Result.DbName, c.Error.DbName,
			c.Id.DbName,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id any
	err = stmt.QueryRow(runAt, tfunc, args, status, startedAt, finishedAt, result, terror).Scan(&id)
	SetFieldValue(v.FieldByName(c.Id.Name), id)

	return err
}

func (r *PostgresTaskRepository2[T]) SetResultAndSave(
	tx *sql.Tx,
	t *T,
	status Status,
	startedAt time.Time,
	finishedAt time.Time,
	result any,
	err string,
) error {
	c := &r.config
	v := reflect.ValueOf(t).Elem()

	SetFieldValue(v.FieldByName(c.Status.Name), status)
	SetFieldValue(v.FieldByName(c.StartedAt.Name), startedAt)
	SetFieldValue(v.FieldByName(c.FinishedAt.Name), finishedAt)
	SetFieldValue(v.FieldByName(c.Result.Name), result)
	SetFieldValue(v.FieldByName(c.Error.Name), err)

	return r.Save(tx, t)
}

func (r *PostgresTaskRepository2[T]) Save(tx *sql.Tx, t *T) error {
	c := &r.config

	v := reflect.ValueOf(t).Elem()

	var id any = v.FieldByName(c.Id.Name).Interface()
	var status any = v.FieldByName(c.Status.Name).Interface()
	var startedAt any = v.FieldByName(c.StartedAt.Name).Interface()
	var finishedAt any = v.FieldByName(c.FinishedAt.Name).Interface()
	var result any = v.FieldByName(c.Result.Name).Interface()
	var terror any = v.FieldByName(c.Error.Name).Interface()

	result, err := json.Marshal(result)
	if err != nil {
		return err
	}

	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s
			set %s=$1, %s=$2, %s=$3, %s=$4, %s=$5
			where %s=$6`,
			c.TableName,
			c.Status.DbName, c.StartedAt.DbName, c.FinishedAt.DbName, c.Result.DbName, c.Error.DbName,
			c.Id.DbName,
		),
		status, startedAt, finishedAt, result, terror,
		id,
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

func (r *PostgresTaskRepository2[T]) DeleteOld(tx *sql.Tx, moment time.Time) (int, error) {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s in ($1, $2) and %s<=$3`,
			c.TableName,
			c.Status.DbName, c.RunAt.DbName,
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

func (r *PostgresTaskRepository2[T]) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ TaskRepository2[Task] = &PostgresTaskRepository2[Task]{}
