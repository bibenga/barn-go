package task

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type TaskFunc func(tx *sql.Tx, args any) (any, error)

type Registry struct {
	funcs map[string]TaskFunc
}

func NewRegistry() *Registry {
	return &Registry{
		funcs: make(map[string]TaskFunc),
	}
}

func (r *Registry) Register(name string, f TaskFunc) {
	r.funcs[name] = f
}

func (r *Registry) Unregister(name string) {
	delete(r.funcs, name)
}

func (r *Registry) Call(tx *sql.Tx, name string, args any) (any, error) {
	// ctx := context.WithValue(context.Background(), "tx", tx)
	f, ok := r.funcs[name]
	if !ok {
		return nil, fmt.Errorf("the function '%s' is not found", name)
	}
	return f(tx, args)
}

func (r *Registry) Delay(tx *sql.Tx, name string, args any, countdown *int, eta *time.Time) error {
	_, ok := r.funcs[name]
	if !ok {
		return fmt.Errorf("the function '%s' is not found", name)
	}
	return errors.New("not implemented")
}
