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
create index if not exists idx_barn_task_run_at on barn_task(run_at);

create table if not exists barn_schedule (
    id serial not null, 
    name varchar not null, 
    is_active boolean default true not null, 
    cron varchar, 
    next_run_at timestamp with time zone, 
    last_run_at timestamp with time zone, 
    func varchar not null,
    args jsonb, 
    primary key (id)
);