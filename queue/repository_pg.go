package queue

import (
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type PostgresQueueRepository struct {
	config QueueQueryConfig
}

func NewPostgresQueueRepository(config ...QueueQueryConfig) QueueRepository {
	var c *QueueQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c = &QueueQueryConfig{}
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
	if c.PayloadField == "" {
		c.PayloadField = DefaultPayloadField
	}
}

func (r *PostgresQueueRepository) CreateTable(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s timestamp with time zone not null, 
				%s jsonb not null, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.CreatedAtField,
			c.PayloadField,
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

func (r *PostgresQueueRepository) FindNext(tx *sql.Tx) (*Message, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s
			from %s
			order by %s
			limit 1
			for update skip locked`,
			c.IdField, c.CreatedAtField, c.PayloadField,
			c.TableName,
			c.CreatedAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var m Message
	row := stmt.QueryRow()
	if err := row.Scan(&m.Id, &m.CreatedAt, &m.Payload); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &m, nil
}

func (r *PostgresQueueRepository) FindManyNext(tx *sql.Tx, limit int) ([]*Message, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s
			from %s
			order by %s
			limit 1
			for update skip locked`,
			c.IdField, c.CreatedAtField, c.PayloadField,
			c.TableName,
			c.CreatedAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []*Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.Id, &m.CreatedAt, &m.Payload); err != nil {
			return nil, err
		}
		messages = append(messages, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (r *PostgresQueueRepository) Create(tx *sql.Tx, m *Message) error {
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s) 
			values ($1, $2) 
			returning %s`,
			c.TableName,
			c.CreatedAtField, c.PayloadField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(&m.CreatedAt, &m.Payload).Scan(&m.Id)
	return err
}

func (r *PostgresQueueRepository) Delete(tx *sql.Tx, m *Message) error {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s=$1`,
			c.TableName,
			c.IdField,
		),
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
		return nil
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

var _ QueueRepository = &PostgresQueueRepository{}
