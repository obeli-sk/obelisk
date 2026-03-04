-- Stores execution history. Append only.
CREATE TABLE IF NOT EXISTS t_execution_log (
    execution_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    json_value TEXT NOT NULL,
    version INTEGER NOT NULL,
    variant TEXT NOT NULL,
    join_set_id TEXT,
    history_event_type TEXT GENERATED ALWAYS AS (json_value->>'$.history_event.event.type') STORED,
    PRIMARY KEY (execution_id, version)
) STRICT;

-- Used in `fetch_created` and `get_execution_event`
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_version  ON t_execution_log (execution_id, version);

-- Used in `lock_inner` to filter by execution ID and variant (created or event history)
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_variant  ON t_execution_log (execution_id, variant);

-- Used in `count_join_next`
CREATE INDEX IF NOT EXISTS idx_t_execution_log_execution_id_join_set  ON t_execution_log (execution_id, join_set_id, history_event_type) WHERE history_event_type="join_next";

-- Stores child execution return values for the parent (`execution_id`). Append only.
CREATE TABLE IF NOT EXISTS t_join_set_response (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    execution_id TEXT NOT NULL,
    join_set_id TEXT NOT NULL,

    delay_id TEXT,
    delay_success INTEGER,

    child_execution_id TEXT,
    finished_version INTEGER,

    UNIQUE (execution_id, join_set_id, delay_id, child_execution_id)
) STRICT;

-- Used when querying for the next response
CREATE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_id ON t_join_set_response (execution_id, id);

-- Child execution id must be unique.
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_child_id
ON t_join_set_response (child_execution_id) WHERE child_execution_id IS NOT NULL;

-- Delay id must be unique.
CREATE UNIQUE INDEX IF NOT EXISTS idx_join_set_response_unique_delay_id
ON t_join_set_response (delay_id) WHERE delay_id IS NOT NULL;

-- Stores executions in `PendingState`
CREATE TABLE IF NOT EXISTS t_state (
    execution_id TEXT NOT NULL,
    is_top_level INTEGER NOT NULL,
    corresponding_version INTEGER NOT NULL,
    ffqn TEXT NOT NULL,
    created_at TEXT NOT NULL,
    component_id_input_digest BLOB NOT NULL,
    component_type TEXT NOT NULL,
    first_scheduled_at TEXT NOT NULL,
    deployment_id TEXT NOT NULL,
    is_paused INTEGER NOT NULL,

    pending_expires_finished TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    intermittent_event_count INTEGER NOT NULL,

    max_retries INTEGER,
    retry_exp_backoff_millis INTEGER,
    last_lock_version INTEGER,
    executor_id TEXT,
    run_id TEXT,

    join_set_id TEXT,
    join_set_closing INTEGER,

    result_kind TEXT,

    PRIMARY KEY (execution_id)
) STRICT;

-- For `get_pending_by_ffqns`
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending_by_ffqn ON t_state (pending_expires_finished, ffqn) WHERE state = 'pending_at';

-- For `get_pending_by_component_input_digest`
CREATE INDEX IF NOT EXISTS idx_t_state_lock_pending_by_component ON t_state (pending_expires_finished, component_id_input_digest) WHERE state = 'pending_at';

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
    expires_at TEXT NOT NULL,
    PRIMARY KEY (execution_id, join_set_id, delay_id)
) STRICT;

-- Backtrace tables. Append only.
CREATE TABLE IF NOT EXISTS t_execution_backtrace (
    execution_id TEXT NOT NULL,
    component_id TEXT NOT NULL,
    version_min_including INTEGER NOT NULL,
    version_max_excluding INTEGER NOT NULL,
    backtrace_hash BLOB NOT NULL,

    PRIMARY KEY (
        execution_id,
        version_min_including,
        version_max_excluding
    ),
    FOREIGN KEY (backtrace_hash)
        REFERENCES t_wasm_backtrace(backtrace_hash)
) STRICT;

-- Index for searching backtraces by execution_id and version
CREATE INDEX IF NOT EXISTS idx_t_execution_backtrace_execution_id_version
ON t_execution_backtrace (
    execution_id,
    version_min_including,
    version_max_excluding
);

-- Deduplication of backtraces
CREATE TABLE IF NOT EXISTS t_wasm_backtrace (
    backtrace_hash BLOB NOT NULL,
    wasm_backtrace TEXT NOT NULL,

    PRIMARY KEY (backtrace_hash)
) STRICT;

-- Stores logs and std stream output of execution runs. Append only.
CREATE TABLE IF NOT EXISTS t_log (
    id INTEGER PRIMARY KEY,
    execution_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    level INTEGER,
    message TEXT,
    stream_type INTEGER,
    payload BLOB
) STRICT;

CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_run_id_created_at ON t_log (execution_id, run_id, created_at);

CREATE INDEX IF NOT EXISTS idx_t_log_execution_id_created_at ON t_log (execution_id, created_at);

-- Content-addressed store for source file text. Content hash is SHA-256 of the UTF-8 content.
CREATE TABLE IF NOT EXISTS t_source_file (
    content_hash BLOB NOT NULL,
    content      TEXT NOT NULL,

    PRIMARY KEY (content_hash)
) STRICT;

-- Maps (component_digest, frame_key, is_suffix) to a source file.
-- frame_key is the exact frame symbol path (is_suffix=0) or a '/'-prefixed suffix (is_suffix=1).
CREATE TABLE IF NOT EXISTS t_component_source (
    component_digest BLOB    NOT NULL,
    frame_key        TEXT    NOT NULL,
    is_suffix        INTEGER NOT NULL,
    content_hash     BLOB    NOT NULL,

    PRIMARY KEY (component_digest, frame_key, is_suffix),
    FOREIGN KEY (content_hash)
        REFERENCES t_source_file(content_hash)
) STRICT;

CREATE TABLE IF NOT EXISTS t_deployment (
    deployment_id TEXT NOT NULL PRIMARY KEY,
    created_at    TEXT NOT NULL,
    last_active_at TEXT,
    status        TEXT NOT NULL,
    config_json      TEXT NOT NULL,
    obelisk_version  TEXT NOT NULL,
    created_by       TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_t_deployment_status ON t_deployment (status);

-- Enforces at most one active deployment at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_t_deployment_single_active ON t_deployment ((1)) WHERE status = 'active';

-- Enforces at most one enqueued deployment at a time.
CREATE UNIQUE INDEX IF NOT EXISTS idx_t_deployment_single_enqueued ON t_deployment ((1)) WHERE status = 'enqueued';
