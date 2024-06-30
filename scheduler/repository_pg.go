package scheduler

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type PostgresSchedulerRepository struct {
	config ScheduleQueryConfig
}

func NewPostgresSchedulerRepository(config ...ScheduleQueryConfig) SchedulerRepository {
	var c *ScheduleQueryConfig
	if len(config) > 0 {
		c = &config[0]
	} else {
		c = &ScheduleQueryConfig{}
	}
	r := &PostgresSchedulerRepository{
		config: *c,
	}
	r.setupDefaults()
	return r
}

func (r *PostgresSchedulerRepository) setupDefaults() {
	c := &r.config
	if c.TableName == "" {
		c.TableName = DefaultTableName
	}
	if c.IdField == "" {
		c.IdField = DefaultIdField
	}
	if c.NameField == "" {
		c.NameField = DefaultNameField
	}
	if c.IsActiveField == "" {
		c.IsActiveField = DefaultIsActiveField
	}
	if c.CronField == "" {
		c.CronField = DefaultCronField
	}
	if c.NextRunAtField == "" {
		c.NextRunAtField = DefaultNextRunAtField
	}
	if c.LastRunAtField == "" {
		c.LastRunAtField = DefaultLastRunAtField
	}
	if c.FuncField == "" {
		c.FuncField = DefaultFuncField
	}
	if c.ArgsField == "" {
		c.ArgsField = DefaultArgsField
	}
}

func (r *PostgresSchedulerRepository) CreateTable(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`create table if not exists %s (
				%s serial not null, 
				%s varchar not null, 
				%s boolean default true not null, 
				%s varchar, 
				%s timestamp with time zone, 
				%s timestamp with time zone, 
				%s varchar not null, 
				%s jsonb, 
				primary key (%s)
			)`,
			c.TableName,
			c.IdField,
			c.NameField,
			c.IsActiveField,
			c.CronField,
			c.NextRunAtField,
			c.LastRunAtField,
			c.FuncField,
			c.ArgsField,
			c.IdField,
		),
	)
	return err
}

func (r *PostgresSchedulerRepository) FindAllActiveAndUnprocessed(tx *sql.Tx, moment time.Time) ([]*Schedule, error) {
	c := &r.config
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`select %s, %s, %s, %s, %s, %s, %s, %s  
			from %s
			where %s and (%s is null or %s < $1)
			order by %s
			limit $2
			for update`,
			c.IdField, c.NameField, c.IsActiveField, c.CronField, c.NextRunAtField, c.LastRunAtField, c.FuncField, c.ArgsField,
			c.TableName,
			c.IsActiveField, c.NextRunAtField, c.NextRunAtField,
			c.NextRunAtField,
		),
	)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, err := stmt.Query(moment)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schedules []*Schedule
	for rows.Next() {
		var s Schedule = Schedule{}
		var payload []byte
		err := rows.Scan(&s.Id, &s.Name, &s.IsActive, &s.Cron, &s.NextRunAt, &s.LastRunAt, &c.FuncField, &payload)
		if err != nil {
			return nil, err
		}
		if payload != nil {
			if err := json.Unmarshal(payload, &s.Args); err != nil {
				return nil, err
			}
		}
		schedules = append(schedules, &s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schedules, nil
}

func (r *PostgresSchedulerRepository) Create(tx *sql.Tx, s *Schedule) error {
	c := &r.config
	if s.Cron == nil && s.NextRunAt == nil {
		return fmt.Errorf("invalid cron and/or nextTs	")
	}
	payload, err := json.Marshal(s.Args)
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(
		fmt.Sprintf(
			`insert into %s(%s, %s, %s, %s, %s) 
			values ($1, $2, $3, $4, $5) 
			returning %s`,
			c.TableName,
			c.NameField, c.CronField, c.NextRunAtField, c.FuncField, c.ArgsField,
			c.IdField,
		),
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	err = stmt.QueryRow(s.Name, s.Cron, s.NextRunAt, s.Func, payload).Scan(&s.Id)
	return err
}

func (r *PostgresSchedulerRepository) Save(tx *sql.Tx, s *Schedule) error {
	c := &r.config
	res, err := tx.Exec(
		fmt.Sprintf(
			`update %s 
			set %s=$1, %s=$2, %s=$3
			where %s=$4`,
			c.TableName,
			c.IsActiveField, c.NextRunAtField, c.LastRunAtField,
			c.IdField,
		),
		s.IsActive, s.NextRunAt, s.LastRunAt,
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

func (r *PostgresSchedulerRepository) Delete(tx *sql.Tx, pk int) error {
	c := &r.config
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

func (r *PostgresSchedulerRepository) DeleteAll(tx *sql.Tx) error {
	c := &r.config
	_, err := tx.Exec(
		fmt.Sprintf(
			`delete from %s`,
			c.TableName,
		),
	)
	return err
}

var _ SchedulerRepository = &PostgresSchedulerRepository{}
