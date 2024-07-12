drop table if exists barn_task_pay;

create table if not exists barn_task_pay (
    id serial not null, 
    run_at timestamp with time zone, 
    -- func varchar not null,
    -- args jsonb, 
    status char(1) default 'Q',
    started_at timestamp with time zone, 
    finished_at timestamp with time zone, 
    -- result jsonb, 
    error varchar, 
    user_id varchar(100) not null,
    amount double precision not null,
    idempotency_key varchar(100) not null,
    attemt integer not null,
    max_attempts integer not null,
    primary key (id)
);

