-- Widen the system-event level constraint to include debug events. SQLite cannot
-- alter a CHECK constraint in place, so replace the column while preserving the
-- table's identity and indexes.
ALTER TABLE t_system_event
ADD COLUMN level_widened TEXT NOT NULL DEFAULT 'info'
    CHECK (level_widened IN ('debug', 'info', 'warning', 'error'));
UPDATE t_system_event SET level_widened = level;
ALTER TABLE t_system_event DROP COLUMN level;
ALTER TABLE t_system_event RENAME COLUMN level_widened TO level;
