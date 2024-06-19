create table if not exists barn_lock  (
    name varchar not null,
    locked_at timestamp default (current_timestamp) not null,
    locked_by varchar not null default '',
    primary key (name)
);

create table if not exists barn_entry (
    id serial not null, 
    name varchar not null, 
    is_active boolean default true not null, 
    cron varchar, 
    next_ts timestamp with time zone, 
    last_ts timestamp with time zone, 
    message jsonb, 
    primary key (id),
    unique (name)
);
