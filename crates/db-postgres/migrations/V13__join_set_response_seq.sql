-- Give each response a per-execution 1-based ordinal `seq`, replacing the global
-- primary key `id` as the public response cursor and the ordering watermark.
-- `id` stays the opaque primary key.
ALTER TABLE t_join_set_response ADD COLUMN seq BIGINT;

-- Backfill: 1-based ordinal per execution, ordered by the existing global id.
WITH numbered AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY execution_id ORDER BY id) AS rn
    FROM t_join_set_response
)
UPDATE t_join_set_response
SET seq = numbered.rn
FROM numbered
WHERE numbered.id = t_join_set_response.id;

-- Enforce uniqueness and serve the (execution_id, seq) watermark queries.
CREATE UNIQUE INDEX IF NOT EXISTS idx_t_join_set_response_execution_id_seq
    ON t_join_set_response (execution_id, seq);
