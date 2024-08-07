-- barn_task
drop table if exists barn_task;

create table if not exists barn_task (
    id serial not null, 
    run_at timestamp with time zone, 
    func varchar not null,
    args jsonb, 
    status char(1) default 'Q',
    started_at timestamp with time zone, 
    finished_at timestamp with time zone, 
    result jsonb, 
    error varchar, 
    primary key (id)
);
create index if not exists barn_task_find_next_idx on barn_task(run_at) where status = 'Q';
create index if not exists barn_task_delete_idx on barn_task(run_at) where status = 'D' or status = 'F';

-- delete from barn_task;

-- barn_schedule
drop table if exists barn_schedule;

create table if not exists barn_schedule (
    id serial not null, 
    name varchar not null, 
    is_active boolean default true not null, 
    next_run_at timestamp with time zone, 
    interval interval,
    cron varchar, 
    last_run_at timestamp with time zone, 
    func varchar not null,
    args jsonb, 
    primary key (id)
);

-- delete from barn_schedule;
