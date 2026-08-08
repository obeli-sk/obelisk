-- The expired-delay sweep (`WHERE expires_at <= $1 AND NOT is_paused`) and the
-- global earliest-wakeup lookup (`MIN(expires_at)`) both full-scanned t_delay,
-- whose primary key does not lead with expires_at. Paused delays never wake, so
-- the index only covers the non-paused rows the queries actually read.
CREATE INDEX IF NOT EXISTS idx_t_delay_expires_at
    ON t_delay (expires_at) WHERE NOT is_paused;
