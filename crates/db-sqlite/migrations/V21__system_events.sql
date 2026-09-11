CREATE TABLE t_system_event (
    event_id TEXT PRIMARY KEY NOT NULL CHECK (length(event_id) <= 64),
    server_run_id TEXT NOT NULL CHECK (length(server_run_id) = 30),
    created_at TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('info', 'warning', 'error')),
    code TEXT NOT NULL CHECK (length(code) <= 64),
    execution_id TEXT CHECK (length(execution_id) <= 1024),
    deployment_id TEXT CHECK (length(deployment_id) <= 64),
    dedupe_key TEXT CHECK (length(dedupe_key) <= 512),
    cas_digest TEXT REFERENCES t_file(digest) CHECK (length(cas_digest) = 71),
    details TEXT NOT NULL CHECK (length(details) <= 4096)
) STRICT;
CREATE INDEX t_system_event_created_idx ON t_system_event(created_at DESC, event_id DESC);
CREATE INDEX t_system_event_code_idx ON t_system_event(code, created_at DESC, event_id DESC);
CREATE INDEX t_system_event_deployment_idx ON t_system_event(deployment_id, created_at DESC, event_id DESC);
CREATE INDEX t_system_event_server_run_idx ON t_system_event(server_run_id, event_id DESC);
CREATE UNIQUE INDEX t_system_event_dedupe_idx
    ON t_system_event(code, deployment_id, dedupe_key)
    WHERE dedupe_key IS NOT NULL;
