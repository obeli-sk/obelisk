CREATE TABLE t_app_checkpoint (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    checkpoint JSONB NOT NULL
);
