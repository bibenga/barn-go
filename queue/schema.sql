create table if not exists barn_queue (
    id serial not null, 
    created_ts timestamp with time zone not null, 
    name varchar not null,
    payload jsonb not null, 
    is_processed_flg boolean default false not null, 
    processed_ts timestamp with time zone, 
    is_success_flg boolean, 
    error varchar, 
    primary key (id)
);

create index if not exists idx_barn_queue_created_ts on barn_queue (created_ts);
