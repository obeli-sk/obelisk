CREATE TABLE t_app_checkpoint (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    checkpoint TEXT NOT NULL CHECK (json_valid(checkpoint))
);
