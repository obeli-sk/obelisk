-- `CURRENT_TIMESTAMP` wrote `YYYY-MM-DD HH:MM:SS`; rusqlite appends a fraction and `+00:00`.
UPDATE t_state SET updated_at = updated_at || '+00:00' WHERE length(updated_at) = 19;
