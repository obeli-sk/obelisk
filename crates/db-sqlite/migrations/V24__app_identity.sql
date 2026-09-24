CREATE TABLE t_app_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    app_name TEXT NOT NULL
);

-- backcompat: 0.41 databases had no name and used the shared default location.
INSERT INTO t_app_identity (singleton, app_name)
SELECT 1, 'default' WHERE EXISTS (SELECT 1 FROM t_deployment)
    OR EXISTS (SELECT 1 FROM t_system_event);
