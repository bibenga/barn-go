package scheduler

import (
	"database/sql"
	"fmt"
	"time"
)

type PostgresSchedulerRepository struct {
	Config *ScheduleQueryConfig
}

func NewPgSchedulerRepository(conig *ScheduleQueryConfig) SchedulerRepository {
	conig.init()
	return &PostgresSchedulerRepository{
		Config: conig,
	}
}

func NewDefaultPostgresSchedulerRepository() SchedulerRepository {
	return NewPgSchedulerRepository(&ScheduleQueryConfig{})
}

func (r *PostgresSchedulerRepository) CreateTable(tx *sql.Tx) error {
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s varchar not null, 
				%s boolean default true not null, 
				%s varchar, 
				%s timestamp with time zone, 
				%s timestamp with time zone, 
				%s jsonb, 
				primary key (%s),
				unique (%s)
			)`,
			c.TableName,
			c.IdField,
			c.NameField,
			c.IsActiveField,
			c.CronField,
			c.NextTsField,
			c.LastTsField,
			c.MessageField,
			c.IdField,
			c.NameField,
		),
	)
	return err
}

func (r *PostgresSchedulerRepository) FindAllActive(tx *sql.Tx) ([]*Schedule, error) {
	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		var s Schedule = Schedule{}
		err := rows.Scan(&s.Id, &s.Name, &s.IsActive, &s.Cron, &s.NextTs, &s.LastTs, &s.Message)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, &s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

func (r *PostgresSchedulerRepository) FindActiveAndExpired(tx *sql.Tx, moment *time.Time, limit int) ([]*Schedule, error) {
	c := r.Config
	if moment == nil {
		m := time.Now().UTC()
		moment = &m
	}
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s and (%s is null or %s < $1)
			order by %s
			limit $2
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField, c.NextTsField, c.NextTsField,
			c.NextTsField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(*moment, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		var s Schedule = Schedule{}
		err := rows.Scan(&s.Id, &s.Name, &s.IsActive, &s.Cron, &s.NextTs, &s.LastTs, &s.Message)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, &s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

func (r *PostgresSchedulerRepository) FindOne(tx *sql.Tx, pk int) (*Schedule, error) {
	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s 
			from %s
			where %s and %s=$1
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextTsField, c.LastTsField, c.MessageField,
			c.TableName,
			c.IsActiveField, c.IdField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	var s Schedule
	row := stmt.QueryRow(pk)
	if err := row.Scan(&s.Id, &s.Name, &s.IsActive, &s.Cron, &s.NextTs, &s.LastTs, &s.Message); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		} else {
			return nil, err
		}
	}
	return &s, nil
}

func (r *PostgresSchedulerRepository) Create(tx *sql.Tx, s *Schedule) error {
	if s.Cron == nil && s.NextTs == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}

	c := r.Config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s) 
			values ($1, $2, $3, $4) 
			returning %s`,
			c.TableName,
			c.NameField, c.CronField, c.NextTsField, c.MessageField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(s.Name, s.Cron, s.NextTs, s.Message).Scan(&s.Id)
	return err
}

func (r *PostgresSchedulerRepository) Save(tx *sql.Tx, s *Schedule) error {
	c := r.Config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3
			where %s=$4`,
			c.TableName,
			c.IsActiveField, c.NextTsField, c.LastTsField,
			c.IdField,
		),
		s.IsActive, s.NextTs, s.LastTs,
		s.Id,
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

func (r *PostgresSchedulerRepository) DeleteAll(tx *sql.Tx) error {
	c := r.Config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

func (r *PostgresSchedulerRepository) Delete(tx *sql.Tx, pk int) error {
	c := r.Config
	res, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s 
			where %s=$1`,
			c.TableName,
			c.IdField,
		),
		pk,
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

var _ SchedulerRepository = &PostgresSchedulerRepository{}
