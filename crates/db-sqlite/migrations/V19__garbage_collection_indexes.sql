-- Orphan cleanup probes these relationships in the referenced-to-referencing
-- direction. The existing primary keys start with other columns, so without
-- these indexes SQLite runs a correlated full-table scan for every candidate.
CREATE INDEX idx_t_execution_backtrace_backtrace_hash
    ON t_execution_backtrace (backtrace_hash);

CREATE INDEX idx_t_deployment_component_component_digest
    ON t_deployment_component (component_digest);
