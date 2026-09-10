CREATE TABLE t_system_event (
    event_id VARCHAR(64) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    level VARCHAR(7) NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    code VARCHAR(64) NOT NULL,
    message VARCHAR(512) NOT NULL,
    execution_id VARCHAR(1024),
    deployment_id VARCHAR(64),
    details JSONB NOT NULL,
    CHECK (pg_column_size(details) <= 4096)
);
CREATE INDEX t_system_event_created_idx ON t_system_event(created_at DESC, event_id DESC);
CREATE INDEX t_system_event_code_idx ON t_system_event(code, created_at DESC, event_id DESC);
CREATE INDEX t_system_event_deployment_idx ON t_system_event(deployment_id, created_at DESC, event_id DESC);
