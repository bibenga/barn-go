create table if not exists barn_task (
    id serial not null, 
    created_ts timestamp with time zone not null, 
    func varchar not null,
    args jsonb, 
    is_processed boolean default false not null, 
    started_at timestamp with time zone, 
    finished_at timestamp with time zone, 
    is_success_flg boolean, 
    result jsonb, 
    error varchar, 
    primary key (id)
);

create index if not exists idx_barn_task_created_ts on barn_task (created_ts);
