CREATE TABLE IF NOT EXISTS t_file (
    digest  TEXT    NOT NULL PRIMARY KEY,
    content BLOB    NOT NULL,
    size    INTEGER NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS t_deployment_file (
    deployment_id TEXT NOT NULL,
    digest        TEXT NOT NULL,
    path          TEXT NOT NULL,

    PRIMARY KEY (deployment_id, digest),
    FOREIGN KEY (deployment_id)
        REFERENCES t_deployment(deployment_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_t_deployment_file_digest ON t_deployment_file (digest);
