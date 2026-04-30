-- Stores execution history. Append only.
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    json_value JSON NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value #>> '{history_event,event,type}') STORED,
    PRIMARY KEY (execution_id, version)
);

CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version ON t_execution_log (execution_id, version);

CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant ON t_execution_log (execution_id, variant);

CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_join_set ON t_execution_log
    (execution_id, join_set_id, history_event_type) WHERE history_event_type='join_next';

-- Stores child execution return values for the parent (`execution_id`). Append only.
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,

    delay_id TEXT,
    delay_success BOOLEAN,

    child_execution_id TEXT,
    finished_version BIGINT CHECK (finished_version >= 0),

    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
);

CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_child_id
ON t_join_set_response (child_execution_id) WHERE child_execution_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_delay_id
ON t_join_set_response (delay_id) WHERE delay_id IS NOT NULL;

-- Stores executions in `PendingState`
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    is_top_level BOOLEAN NOT NULL,
    corresponding_version BIGINT NOT NULL CHECK (corresponding_version >= 0),
    ffqn TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    component_id_input_digest BYTEA NOT NULL,
    component_type TEXT NOT NULL,
    first_scheduled_at TIMESTAMPTZ NOT NULL,
    deployment_id TEXT NOT NULL,
    is_paused BOOLEAN NOT NULL,

    pending_expires_finished TIMESTAMPTZ NOT NULL,
    state TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    intermittent_event_count BIGINT NOT NULL CHECK (intermittent_event_count >=0),

    max_retries BIGINT CHECK (max_retries >= 0),
    retry_exp_backoff_millis BIGINT CHECK (retry_exp_backoff_millis >= 0),
    last_lock_version BIGINT CHECK (last_lock_version >= 0),
    executor_id TEXT,
    run_id TEXT,

    join_set_id TEXT,
    join_set_closing BOOLEAN,

    result_kind JSONB,

    PRIMARY KEY (execution_id)
);

-- For `get_pending_of_single_ffqn`
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending_by_ffqn ON t_state (state, pending_expires_finished, ffqn) WHERE state = 'pending_at';

-- For `get_pending_by_component_input_digest`
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending_by_component ON t_state (state, pending_expires_finished, component_id_input_digest) WHERE state = 'pending_at';

CREATE INDEX IF NOT EXISTS idx_t_state_expired_locks ON t_state (pending_expires_finished) WHERE state = 'locked';

CREATE INDEX IF NOT EXISTS idx_t_state_execution_id_is_root ON t_state (execution_id, is_top_level);

-- For `list_executions` by ffqn
CREATE INDEX IF NOT EXISTS idx_t_state_ffqn ON t_state (ffqn);

-- For `list_executions` by creation date
CREATE INDEX IF NOT EXISTS idx_t_state_created_at ON t_state (created_at);

-- For `list_deployment_states`
CREATE INDEX IF NOT EXISTS idx_t_state_deployment_state ON t_state (deployment_id, state);

-- Represents `ExpiredTimer::AsyncDelay`. Rows are deleted when the delay is processed.
CREATE TABLE IF NOT EXISTS t_delay (
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,
    delay_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
);

-- Deduplication of backtraces
CREATE TABLE IF NOT EXISTS t_wasm_backtrace (
    backtrace_hash BYTEA PRIMARY KEY,
    wasm_backtrace JSONB NOT NULL
);

-- Backtrace tables. Append only.
CREATE TABLE IF NOT EXISTS t_execution_backtrace (
    execution_id TEXT NOT NULL,
    component_id JSONB NOT NULL,
    version_min_including BIGINT NOT NULL CHECK (version_min_including >= 0),
    version_max_excluding BIGINT NOT NULL CHECK (version_max_excluding >= 0),
    backtrace_hash BYTEA NOT NULL,
    PRIMARY KEY (execution_id, version_min_including, version_max_excluding),
    FOREIGN KEY (backtrace_hash) REFERENCES t_wasm_backtrace(backtrace_hash)
);

CREATE INDEX IF NOT EXISTS idx_t_execution_backtrace_execution_id_version ON t_execution_backtrace (execution_id, version_min_including, version_max_excluding);

-- Content-addressed store for source file text. Content hash is SHA-256 of the UTF-8 content.
CREATE TABLE IF NOT EXISTS t_source_file (
    content_hash BYTEA PRIMARY KEY,
    content      TEXT NOT NULL
);

-- Maps (component_digest, frame_key, is_suffix) to a source file.
CREATE TABLE IF NOT EXISTS t_component_source (
    component_digest BYTEA   NOT NULL,
    frame_key        TEXT    NOT NULL,
    is_suffix        BOOLEAN NOT NULL,
    content_hash     BYTEA   NOT NULL,
    PRIMARY KEY (component_digest, frame_key, is_suffix),
    FOREIGN KEY (content_hash) REFERENCES t_source_file(content_hash)
);

-- Stores logs and std stream output of execution runs. Append only.
CREATE TABLE IF NOT EXISTS t_log (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    level INTEGER,
    message TEXT,
    stream_type INTEGER,
    payload BYTEA
);

CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_run_id_created_at ON t_log (execution_id, run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_created_at ON t_log (execution_id, created_at);

CREATE TABLE IF NOT EXISTS t_deployment (
    deployment_id TEXT     NOT NULL PRIMARY KEY,
    created_at    TIMESTAMPTZ NOT NULL,
    last_active_at TIMESTAMPTZ,
    status        TEXT     NOT NULL DEFAULT 'inactive',
    config_json      TEXT     NOT NULL,
    obelisk_version  TEXT     NOT NULL,
    created_by       TEXT
);

CREATE INDEX IF NOT EXISTS idx_t_deployment_status ON t_deployment (status);

-- Enforces at most one active deployment at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_t_deployment_single_active ON t_deployment ((1)) WHERE status = 'active';

-- Enforces at most one enqueued deployment at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_t_deployment_single_enqueued ON t_deployment ((1)) WHERE status = 'enqueued';
