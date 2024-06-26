create table if not exists barn_lock  (
    name varchar not null,
    locked_at_ts timestamp with time zone,
    owner varchar,
    primary key (name)
);
