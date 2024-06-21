CREATE TABLE IF NOT EXISTS barn_lock  (
    name VARCHAR NOT NULL,
    locked_at TIMESTAMP WITH TIME ZONE,
    owner VARCHAR,
    PRIMARY KEY (%s)
);
