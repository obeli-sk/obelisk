-- Orphan cleanup probes these relationships in the referenced-to-referencing
-- direction. The existing indexes start with other columns.
CREATE INDEX idx_t_execution_backtrace_backtrace_hash
    ON t_execution_backtrace (backtrace_hash);

CREATE INDEX idx_t_deployment_component_component_digest
    ON t_deployment_component (component_digest);
