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
	query := fmt.Sprintf(`
		create table if not exists %s (
			id serial not null, 
			run_at timestamp with time zone, 
			func varchar not null,
			args jsonb, 
			status char(1) default 'Q',
			started_at timestamp with time zone, 
			finished_at timestamp with time zone, 
			result jsonb, 
			error varchar, 
			primary key (id)
		);
		create index if not exists idx_%s_created_ts on %s(created_ts);`,
		c.TableName,
		c.TableName,
		c.TableName,
	)
	_, err := tx.Exec(query)
	return err
}

func (r *PostgresTaskRepository2[T]) FindNext(tx *sql.Tx) (*T, error) {
	c := &r.config

	var fields []string
	for _, f := range c.Fields {
		fields = append(fields, f.DbName)
	}

	query := fmt.Sprintf(
		`select %s
		from %s
		where %s = $1 and %s < $2
		order by %s
		limit 1
		for update skip locked`,
		strings.Join(fields, ", "),
		c.TableName,
		c.FieldsByName["Status"].DbName, c.FieldsByName["RunAt"].DbName,
		c.FieldsByName["RunAt"].DbName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	row := stmt.QueryRow(Queued, time.Now().UTC())
	values := make([]any, len(fields))
	valuesHolder := make([]any, len(fields))
	for i := range values {
		valuesHolder[i] = &values[i]
	}
	if err := row.Scan(valuesHolder...); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}

	var t = new(T)
	v := reflect.ValueOf(t).Elem()

	for pos, f := range c.Fields {
		value := values[pos]
		if f.Name == "Args" || f.Name == "Result" {
			bytes := value.([]byte)
			if err := json.Unmarshal(bytes, &value); err != nil {
				return nil, err
			}
		}
		SetFieldValue(v.FieldByName(f.StructName), value)
	}

	return t, nil
}

func (r *PostgresTaskRepository2[T]) Create(tx *sql.Tx, t *T) error {
	c := &r.config

	tv := reflect.ValueOf(t).Elem()

	var fields []string
	var valuesHolder []string
	var values []any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			continue
		}
		fields = append(fields, f.DbName)
		valuesHolder = append(valuesHolder, fmt.Sprintf("$%d", idx))
		value := tv.FieldByName(f.StructName).Interface()

		if f.Name == "RunAt" {
			if v, ok := value.(time.Time); ok {
				if v.IsZero() {
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			} else if v, ok := value.(*time.Time); ok {
				if v == nil {
					// v1 := time.Now().UTC()
					// value = &v1
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			}
		} else if f.Name == "Status" {
			if v, ok := value.(Status); ok {
				if v == "" {
					value = Queued
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			} else {
				value = Queued
				SetFieldValue(tv.FieldByName(f.StructName), value)
			}

		} else if f.Name == "Args" || f.Name == "Result" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}

		values = append(values, value)
		idx++
	}

	// query
	query := fmt.Sprintf(
		`insert into %s(%s) 
		values (%s) 
		returning %s`,
		c.TableName,
		strings.Join(fields, ", "),
		strings.Join(valuesHolder, ", "),
		c.FieldsByName["Id"].DbName,
	)
	stmt, err := tx.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	var id any
	err = stmt.QueryRow(values...).Scan(&id)
	SetFieldValue(tv.FieldByName(c.FieldsByName["Id"].StructName), id)

	return err
}

func (r *PostgresTaskRepository2[T]) Save(tx *sql.Tx, t *T) error {
	c := &r.config

	tv := reflect.ValueOf(t).Elem()

	var fields []string
	var values []any
	var idValue any
	idx := 1
	for _, f := range c.Fields {
		if f.Name == "Id" {
			idValue = tv.FieldByName(f.StructName).Interface()
			continue
		}
		fields = append(fields, fmt.Sprintf("%s=$%d", f.DbName, idx))
		value := tv.FieldByName(f.StructName).Interface()

		if f.Name == "RunAt" {
			if v, ok := value.(time.Time); ok {
				if v.IsZero() {
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			} else if v, ok := value.(*time.Time); ok {
				if v == nil {
					// v1 := time.Now().UTC()
					// value = &v1
					value = time.Now().UTC()
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			}
		} else if f.Name == "Status" {
			if v, ok := value.(Status); ok {
				if v == "" {
					value = Queued
					SetFieldValue(tv.FieldByName(f.StructName), value)
				}
			} else {
				value = Queued
				SetFieldValue(tv.FieldByName(f.StructName), value)
			}

		} else if f.Name == "Args" || f.Name == "Result" {
			var err error
			value, err = json.Marshal(value)
			if err != nil {
				return err
			}
		}

		values = append(values, value)
		idx++
	}

	values = append(values, idValue)
	query := fmt.Sprintf(
		`update %s
		set %s
		where %s=$%d`,
		c.TableName,
		strings.Join(fields, ", "),
		c.FieldsByName["Id"].DbName, len(values),
	)
	res, err := tx.Exec(query, values...)
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
			c.FieldsByName["Status"].DbName, c.FieldsByName["RunAt"].DbName,
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
