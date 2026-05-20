CREATE TABLE IF NOT EXISTS t_component_metadata (
    component_digest BLOB NOT NULL,
    imports_json     TEXT NOT NULL,
    exports_json     TEXT NOT NULL,
    wit              TEXT NOT NULL,
    wit_origin       TEXT NOT NULL,
    PRIMARY KEY (component_digest)
) STRICT;

CREATE TABLE IF NOT EXISTS t_deployment_component (
    deployment_id    TEXT NOT NULL,
    component_name   TEXT NOT NULL,
    component_type   TEXT NOT NULL,
    component_digest BLOB NOT NULL,
    PRIMARY KEY (deployment_id, component_name),
    FOREIGN KEY (deployment_id) REFERENCES t_deployment(deployment_id),
    FOREIGN KEY (component_digest) REFERENCES t_component_metadata(component_digest)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_t_deployment_component_deployment_list
ON t_deployment_component (deployment_id, component_type, component_name);

CREATE INDEX IF NOT EXISTS idx_t_deployment_component_deployment_digest
ON t_deployment_component (deployment_id, component_digest);
