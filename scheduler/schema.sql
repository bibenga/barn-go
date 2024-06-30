create table if not exists barn_schedule (
    id serial not null, 
    name varchar not null, 
    is_active_flg boolean default true not null, 
    cron varchar, 
    next_run_ts timestamp with time zone, 
    last_run_ts timestamp with time zone, 
    func varchar not null,
    args jsonb, 
    primary key (id)
);
