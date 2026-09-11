CREATE TABLE t_system_event (
    event_id VARCHAR(64) PRIMARY KEY,
    server_run_id VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    level VARCHAR(7) NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    code VARCHAR(64) NOT NULL,
    execution_id VARCHAR(1024),
    deployment_id VARCHAR(64),
    dedupe_key VARCHAR(512),
    cas_digest VARCHAR(71) REFERENCES t_file(digest),
    details JSONB NOT NULL,
    CHECK (pg_column_size(details) <= 4096)
);
CREATE INDEX t_system_event_created_idx ON t_system_event(created_at DESC, event_id DESC);
CREATE INDEX t_system_event_code_idx ON t_system_event(code, created_at DESC, event_id DESC);
CREATE INDEX t_system_event_deployment_idx ON t_system_event(deployment_id, created_at DESC, event_id DESC);
CREATE INDEX t_system_event_server_run_idx ON t_system_event(server_run_id, event_id DESC);
CREATE UNIQUE INDEX t_system_event_dedupe_idx
    ON t_system_event(code, deployment_id, dedupe_key)
    WHERE dedupe_key IS NOT NULL;
