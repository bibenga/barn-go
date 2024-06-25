package queue

import (
	"database/sql"
	"fmt"
	"time"
)

type PostgresMessageRepository struct {
	Config *MessageQueryConfig
}

func NewPostgresMessageRepository(conig *MessageQueryConfig) MessageRepository {
	conig.init()
	return &PostgresMessageRepository{
		Config: conig,
	}
}

func NewDefaultPostgresMessageRepository() MessageRepository {
	return NewPostgresMessageRepository(&MessageQueryConfig{})
}

func (r *PostgresMessageRepository) CreateTable(tx *sql.Tx) error {
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s varchar, 
				%s timestamp with time zone not null, 
				%s jsonb not null, 
				%s boolean default false not null, 
				%s timestamp with time zone, 
				%s boolean, 
				%s varchar, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.QueueField,
			c.CreatedTsField,
			c.PayloadField,
			c.IsProcessedField,
			c.ProcessedTsField,
			c.IsSuccessField,
			c.ErrorField,
			c.IdField,
		),
	)
	return err
}

func (r *PostgresMessageRepository) FindNext(tx *sql.Tx) (*Message, error) {
	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s
			from %s
			where not %s
			order by %s
			limit 1
			for update`,
			c.IdField, c.QueueField, c.CreatedTsField, c.PayloadField, c.IsProcessedField, c.ProcessedTsField, c.IsSuccessField, c.ErrorField,
			c.TableName,
			c.IsProcessedField,
			c.CreatedTsField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	var m Message
	row := stmt.QueryRow()
	if err := row.Scan(&m.Id, &m.Queue, &m.CreatedTs, &m.Payload, &m.IsProcessed, &m.ProcessedTs, &m.IsSuccess, &m.Error); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &m, nil
}

func (r *PostgresMessageRepository) Create(tx *sql.Tx, m *Message) error {
	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5, $6, $7) 
			returning %s`,
			c.TableName,
			c.QueueField, c.CreatedTsField, c.PayloadField, c.IsProcessedField, c.ProcessedTsField, c.IsSuccessField, c.ErrorField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(&m.Queue, &m.CreatedTs, &m.Payload, &m.IsProcessed, &m.ProcessedTs, &m.IsSuccess, &m.Error).Scan(&m.Id)
	return err
}

func (r *PostgresMessageRepository) Save(tx *sql.Tx, m *Message) error {
	c := r.Config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3, %s=$4
			where %s=$5`,
			c.TableName,
			c.IsProcessedField, c.ProcessedTsField, c.IsSuccessField, c.ErrorField,
			c.IdField,
		),
		m.IsProcessed, m.ProcessedTs, m.IsSuccess, m.Error,
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
	c := r.Config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s and %s<=$1`,
			c.TableName,
			c.IsProcessedField, c.CreatedTsField,
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
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ MessageRepository = &PostgresMessageRepository{}
