-- Earlier enqueue demotions and V20 wrote `inactive_at` with a `T` separator; rusqlite writes a space.
UPDATE t_deployment SET inactive_at = replace(inactive_at, 'T', ' ') WHERE inactive_at LIKE '____-__-__T%';

ALTER TABLE t_deployment ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';
UPDATE t_deployment
SET updated_at = max(created_at, coalesce(last_active_at, created_at), coalesce(inactive_at, created_at));
