package barngo

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
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
	Name       string // a barn's name
	AttrName   string // a field name in a user structure
	ColumnName string // a name in a DB
}

type TableMeta struct {
	TableName    string
	Fields       []*FieldMeta
	FieldsByName map[string]*FieldMeta
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
		FieldsByName: make(map[string]*FieldMeta),
	}
	if tabler, ok := t.(Tabler); ok {
		meta.TableName = tabler.TableName()
	} else {
		meta.TableName = camelToSnake(tt.Name())
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
			dbName = camelToSnake(f.Name)
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
			AttrName:   f.Name,
			ColumnName: dbName,
		}
		meta.Fields = append(meta.Fields, &fieldConfig)
		meta.FieldsByName[fieldName] = &fieldConfig
	}
	// TODO: check required fields
	// if meta.Id.Name == "" || meta.Id.DbName == "" {
	// 	panic(errors.New("id field is not found"))
	// }
	return meta
}

func (m *TableMeta) Has(name string) bool {
	_, ok := m.FieldsByName[name]
	return ok
}

func (m *TableMeta) GetValue(model reflect.Value, name string) (any, bool) {
	if f, ok := m.FieldsByName[name]; ok {
		return model.FieldByName(f.AttrName).Interface(), true
	} else {
		return nil, false
	}
}

func (m *TableMeta) SetValue(model reflect.Value, name string, value any) bool {
	if f, ok := m.FieldsByName[name]; ok {
		setFieldValue(model.FieldByName(f.AttrName), value)
		return true
	} else {
		return false
	}
}

var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

func camelToSnake(name string) string {
	snake := matchFirstCap.ReplaceAllString(name, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func setFieldValue(field reflect.Value, value any) {
	vValue := reflect.ValueOf(value)
	if field.Kind() == reflect.Pointer {
		if value == nil || (vValue.Kind() == reflect.Pointer && vValue.IsNil()) {
			field.SetZero()
		} else if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
			setFieldValue(field.Elem(), value)
		} else {
			setFieldValue(field.Elem(), value)
		}
	} else {
		if value == nil || (vValue.Kind() == reflect.Pointer && vValue.IsNil()) {
			field.SetZero()
		} else {
			if vValue.Type().AssignableTo(field.Type()) {
				field.Set(vValue)
			} else if vValue.Type().ConvertibleTo(field.Type()) {
				field.Set(vValue.Convert(field.Type()))
			} else if field.Type().Name() == "Duration" && vValue.Kind() == reflect.String {
				sValue := value.(string)
				parts := strings.Split(sValue, ":")
				if len(parts) != 3 {
					panic(errors.New("can't set value"))
				}
				hour, err := strconv.Atoi(parts[0])
				if err != nil {
					panic(err)
				}
				minute, err := strconv.Atoi(parts[1])
				if err != nil {
					panic(err)
				}
				second, err := strconv.Atoi(parts[2])
				if err != nil {
					panic(err)
				}
				// dur := ((hour*24 + minute) + second) * time.Second
				dur := time.Duration(hour)*time.Hour +
					time.Duration(minute)*time.Minute +
					time.Duration(second)*time.Second
				setFieldValue(field, dur)
			} else {
				panic(errors.New("can't set value"))
			}
		}
	}
}
