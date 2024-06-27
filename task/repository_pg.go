package task

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type PostgresMessageRepository struct {
	config MessageQueryConfig
}

func NewPostgresMessageRepository(config ...MessageQueryConfig) MessageRepository {
	var c *MessageQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c = &MessageQueryConfig{}
	}
	r := &PostgresMessageRepository{
		config: *c,
	}
	r.setupDefaults()
	return r
}

func (r *PostgresMessageRepository) setupDefaults() {
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
	if c.NameField == "" {
		c.NameField = DefaultNameField
	}
	if c.PayloadField == "" {
		c.PayloadField = DefaultPayloadField
	}
	if c.IsProcessedField == "" {
		c.IsProcessedField = DefaultIsProcessedField
	}
	if c.ProcessedAtField == "" {
		c.ProcessedAtField = DefaultProcessedAtField
	}
	if c.IsSuccessField == "" {
		c.IsSuccessField = DefaultIsSuccessField
	}
	if c.ErrorField == "" {
		c.ErrorField = DefaultErrorField
	}
}

func (r *PostgresMessageRepository) CreateTable(tx *sql.Tx) error {
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
				%s boolean, 
				%s varchar, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.CreatedAtField,
			c.NameField,
			c.PayloadField,
			c.IsProcessedField,
			c.ProcessedAtField,
			c.IsSuccessField,
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
			strings.ReplaceAll(c.TableName, ".", "_"), c.CreatedAtField, c.TableName, c.CreatedAtField,
		),
	)
	return err
}

func (r *PostgresMessageRepository) FindNext(tx *sql.Tx) (*Message, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s
			from %s
			where not %s
			order by %s
			limit 1
			for update`,
			c.IdField, c.CreatedAtField, c.NameField, c.PayloadField, c.IsProcessedField, c.ProcessedAtField, c.IsSuccessField, c.ErrorField,
			c.TableName,
			c.IsProcessedField,
			c.CreatedAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var m Message
	row := stmt.QueryRow()
	if err := row.Scan(&m.Id, &m.CreatedAt, &m.Name, &m.Payload, &m.IsProcessed, &m.ProcessedAt, &m.IsSuccess, &m.Error); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &m, nil
}

func (r *PostgresMessageRepository) Create(tx *sql.Tx, m *Message) error {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5, $6, $7) 
			returning %s`,
			c.TableName,
			c.CreatedAtField, c.NameField, c.PayloadField, c.IsProcessedField, c.ProcessedAtField, c.IsSuccessField, c.ErrorField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(&m.CreatedAt, &m.Name, &m.Payload, &m.IsProcessed, &m.ProcessedAt, &m.IsSuccess, &m.Error).Scan(&m.Id)
	return err
}

func (r *PostgresMessageRepository) Save(tx *sql.Tx, m *Message) error {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3, %s=$4
			where %s=$5`,
			c.TableName,
			c.IsProcessedField, c.ProcessedAtField, c.IsSuccessField, c.ErrorField,
			c.IdField,
		),
		m.IsProcessed, m.ProcessedAt, m.IsSuccess, m.Error,
		m.Id,
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

func (r *PostgresMessageRepository) DeleteProcessed(tx *sql.Tx, t time.Time) (int, error) {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s and %s<=$1`,
			c.TableName,
			c.IsProcessedField, c.CreatedAtField,
		),
		t,
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

func (r *PostgresMessageRepository) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ MessageRepository = &PostgresMessageRepository{}
