CREATE TABLE IF NOT EXISTS barn_schedule (
    id SERIAL NOT NULL, 
    name VARCHAR NOT NULL, 
    is_active BOOLEAN DEFAULT TRUE NOT NULL, 
    cron VARCHAR, 
    next_ts TIMESTAMP WITH TIME ZONE, 
    last_ts TIMESTAMP WITH TIME ZONE, 
    message JSONB, 
    PRIMARY KEY (id),
    UNIQUE (name)
);
