-- The deployment's source of truth is now the verbatim `deployment.toml` manifest,
-- not the resolved canonical config JSON. Existing rows hold JSON and will not
-- activate (breaking change, no back-compat migration, per design).
ALTER TABLE t_deployment RENAME COLUMN config_json TO deployment_toml;
