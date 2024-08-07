package barngo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type TaskFunc func(tx *sql.Tx, args any) (any, error)

type TaskRegistry struct {
	funcs map[string]TaskFunc
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		funcs: make(map[string]TaskFunc),
	}
}

func (r *TaskRegistry) Register(name string, f TaskFunc) {
	r.funcs[name] = f
}

func (r *TaskRegistry) Unregister(name string) {
	delete(r.funcs, name)
}

func (r *TaskRegistry) Call(tx *sql.Tx, name string, args any) (any, error) {
	// ctx := context.WithValue(context.Background(), "tx", tx)
	f, ok := r.funcs[name]
	if !ok {
		return nil, fmt.Errorf("the function '%s' is not found", name)
	}
	return f(tx, args)
}

func (r *TaskRegistry) Delay(tx *sql.Tx, name string, args any, countdown *int, eta *time.Time) error {
	_, ok := r.funcs[name]
	if !ok {
		return fmt.Errorf("the function '%s' is not found", name)
	}
	return errors.New("not implemented")
}

func (r *TaskRegistry) ApplyAsync(tx *sql.Tx, name string, args any, countdown *int, eta *time.Time) error {
	_, ok := r.funcs[name]
	if !ok {
		return fmt.Errorf("the function '%s' is not found", name)
	}
	return errors.New("not implemented")
}

func DefaultRegistryTaskHandler(registry *TaskRegistry) TaskHandler[Task] {
	handler := func(tx *sql.Tx, task *Task) (any, error) {
		return registry.Call(tx, task.Func, task.Args)
	}
	return handler
}
