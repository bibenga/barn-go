package task

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"regexp"
	"strings"
	"time"
)

type Status string

const (
	Queued Status = "Q"
	Done   Status = "D"
	Failed Status = "F"
)

const DefaultTableName = "barn_task"
const DefaultIdField = "id"
const DefaultRunAtField = "run_at"
const DefaultFuncField = "func"
const DefaultArgsField = "args"
const DefaultStatusField = "status"
const DefaultStartedAtField = "started_at"
const DefaultFinishedAtField = "finished_at"
const DefaultResultField = "result"
const DefaultErrorField = "error"

type FieldConfig struct {
	// Field  reflect.StructField
	Name   string
	DbName string
}

type TaskQueryConfig struct {
	TableName       string
	IdField         string
	RunAtField      string
	FuncField       string
	ArgsField       string
	StatusField     string
	StartedAtField  string
	FinishedAtField string
	ResultField     string
	ErrorField      string

	// TableName       string
	Id         FieldConfig
	RunAt      FieldConfig
	Func       FieldConfig
	Args       FieldConfig
	Status     FieldConfig
	StartedAt  FieldConfig
	FinishedAt FieldConfig
	Result     FieldConfig
	Error      FieldConfig

	fields   map[string]string
	dbFields map[string]string
}

type Tabler interface {
	TableName() string
}

type Task struct {
	Id         int        `barn:"id"`
	RunAt      time.Time  `barn:""`
	Func       string     `barn:"func"`
	Args       any        `barn:"Args args"`
	Status     Status     `barn:"status"`
	StartedAt  *time.Time `barn:"started_at"`
	FinishedAt *time.Time `barn:"finished_at"`
	Result     any        `barn:"result"`
	Error      *string    `barn:"error"`
}

func (e Task) TableName() string {
	return "barn_task"
}

func (e Task) LogValue() slog.Value {
	var args []slog.Attr
	args = append(args, slog.Int("Id", e.Id))
	args = append(args, slog.Time("RunAt", e.RunAt))
	args = append(args, slog.String("Func", e.Func))
	args = append(args, slog.Any("Args", e.Args))
	args = append(args, slog.Any("Status", e.Status))
	if e.StartedAt == nil {
		args = append(args, slog.Any("StartedAt", nil))
	} else {
		args = append(args, slog.Time("StartedAt", *e.StartedAt))
	}
	if e.FinishedAt == nil {
		args = append(args, slog.Any("FinishedAt", nil))
	} else {
		args = append(args, slog.Time("FinishedAt", *e.FinishedAt))
	}
	args = append(args, slog.Any("Result", e.Result))
	if e.Error == nil {
		args = append(args, slog.Any("Error", nil))
	} else {
		args = append(args, slog.String("Error", *e.Error))
	}
	return slog.GroupValue(args...)
}

type TaskRepository interface {
	FindNext(tx *sql.Tx) (*Task, error)
	Create(tx *sql.Tx, task *Task) error
	Save(tx *sql.Tx, task *Task) error
	DeleteOld(tx *sql.Tx, t time.Time) (int, error)
}

type TaskRepository2[T any] interface {
	FindNext(tx *sql.Tx) (*T, error)
	Create(tx *sql.Tx, task *T) error
	// SetResultAndSave(tx *sql.Tx, t *T, status Status, startedAt time.Time, finishedAt time.Time, result any, err string) error
	Save(tx *sql.Tx, task *T) error
	DeleteOld(tx *sql.Tx, t time.Time) (int, error)
}

func TaskModelMeta(t interface{}) TaskQueryConfig {
	tt := reflect.TypeOf(t)
	if tt.Kind() == reflect.Pointer {
		tt = tt.Elem()
	}
	if tt.Kind() != reflect.Struct {
		panic(errors.New("invalid value"))
	}

	meta := TaskQueryConfig{}
	if tabler, ok := t.(Tabler); ok {
		meta.TableName = tabler.TableName()
	} else {
		meta.TableName = CamelToSnake(tt.Name())
	}
	fields := reflect.VisibleFields(tt)
	for _, f := range fields {
		tag := f.Tag.Get("barn")
		var fieldName, dbName string
		if tag == "" {
			fieldName = f.Name
			dbName = CamelToSnake(f.Name)
		} else {
			names := strings.Fields(tag)
			if len(names) == 1 {
				fieldName = f.Name
				dbName = names[0]
			} else if len(names) == 2 {
				fieldName = names[0]
				dbName = names[1]
			} else {
				panic(fmt.Errorf("invalid field tag value: %s - %s", f.Name, tag))
			}
		}
		switch {
		case fieldName == "Id":
			meta.Id = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "RunAt":
			meta.RunAt = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "Func":
			meta.Func = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "Args":
			meta.Args = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "Status":
			meta.Status = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "StartedAt":
			meta.StartedAt = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "FinishedAt":
			meta.FinishedAt = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "Result":
			meta.Result = FieldConfig{Name: f.Name, DbName: dbName}
		case fieldName == "Error":
			meta.Error = FieldConfig{Name: f.Name, DbName: dbName}
		}
	}
	if meta.Id.Name == "" || meta.Id.DbName == "" {
		panic(errors.New("id field is not found"))
	}
	return meta
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func CamelToSnake(name string) string {
	snake := matchFirstCap.ReplaceAllString(name, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func SetFieldValue(field reflect.Value, value any) {
	if field.Kind() == reflect.Pointer {
		if value == nil {
			field.SetZero()
		} else if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
			SetFieldValue(field.Elem(), value)
		}
	} else {
		if value == nil {
			field.SetZero()
		} else {
			vValue := reflect.ValueOf(value)
			if vValue.Type().AssignableTo(field.Type()) {
				field.Set(vValue)
			} else if vValue.Type().ConvertibleTo(field.Type()) {
				field.Set(vValue.Convert(field.Type()))
			} else {
				panic(errors.New("jopa"))
			}
		}
	}
}
