CREATE TABLE t_workflow_snapshot_oversized (
    execution_id              TEXT   PRIMARY KEY,
    version                   BIGINT NOT NULL,
    component_digest          BYTEA  NOT NULL,
    prepared_component_digest TEXT   NOT NULL,
    size_bytes                BIGINT NOT NULL,

    FOREIGN KEY (execution_id) REFERENCES t_state(execution_id) ON DELETE CASCADE
);
