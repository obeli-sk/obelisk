CREATE TABLE IF NOT EXISTS t_workflow_snapshot (
    execution_id              TEXT   NOT NULL,
    version                   BIGINT NOT NULL,
    component_digest          BYTEA  NOT NULL,
    prepared_component_digest TEXT   NOT NULL,
    snapshot_digest           TEXT   NOT NULL,

    PRIMARY KEY (execution_id, version),
    FOREIGN KEY (execution_id) REFERENCES t_state(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (snapshot_digest) REFERENCES t_file(digest)
);

CREATE INDEX IF NOT EXISTS idx_workflow_snapshot_latest
    ON t_workflow_snapshot(execution_id, component_digest, version DESC);
