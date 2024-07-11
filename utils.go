package barngo

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

func RunInTransaction(db *sql.DB, f func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = f(tx)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

type FieldMeta struct {
	Name       string
	StructName string
	DbName     string
}

type TableMeta struct {
	TableName    string
	Fields       []FieldMeta
	FieldsByName map[string]FieldMeta
}

type Tabler interface {
	TableName() string
}

func GetTableMeta(t interface{}) TableMeta {
	tt := reflect.TypeOf(t)
	if tt.Kind() == reflect.Pointer {
		tt = tt.Elem()
	}
	if tt.Kind() != reflect.Struct {
		panic(errors.New("invalid value"))
	}

	meta := TableMeta{
		FieldsByName: make(map[string]FieldMeta),
	}
	if tabler, ok := t.(Tabler); ok {
		meta.TableName = tabler.TableName()
	} else {
		meta.TableName = CamelToSnake(tt.Name())
	}
	fields := reflect.VisibleFields(tt)
	for _, f := range fields {
		tag, ok := f.Tag.Lookup("barn")
		if !ok {
			continue
		}
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
		fieldConfig := FieldMeta{
			Name:       fieldName,
			StructName: f.Name,
			DbName:     dbName,
		}
		meta.Fields = append(meta.Fields, fieldConfig)
		meta.FieldsByName[fieldName] = fieldConfig
	}
	// TODO: check required fields
	// if meta.Id.Name == "" || meta.Id.DbName == "" {
	// 	panic(errors.New("id field is not found"))
	// }
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
	vValue := reflect.ValueOf(value)
	if field.Kind() == reflect.Pointer {
		if value == nil || (vValue.Kind() == reflect.Pointer && vValue.IsNil()) {
			field.SetZero()
		} else if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
			SetFieldValue(field.Elem(), value)
		} else {
			SetFieldValue(field.Elem(), value)
		}
	} else {
		if value == nil || (vValue.Kind() == reflect.Pointer && vValue.IsNil()) {
			field.SetZero()
		} else {
			if vValue.Type().AssignableTo(field.Type()) {
				field.Set(vValue)
			} else if vValue.Type().ConvertibleTo(field.Type()) {
				field.Set(vValue.Convert(field.Type()))
			} else {
				panic(errors.New("can't set value"))
			}
		}
	}
}
