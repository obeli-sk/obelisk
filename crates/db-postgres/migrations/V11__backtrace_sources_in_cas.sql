-- Consolidate backtrace source blobs into the content-addressed store (t_file).
-- t_component_source now references t_file by digest; the standalone
-- t_source_file blob store is removed. Existing source data is discarded;
-- sources are repopulated on the next deployment submission.

DROP TABLE IF EXISTS t_component_source;
DROP TABLE IF EXISTS t_source_file;

-- Maps (component_digest, frame_key, is_suffix) to a blob in t_file.
CREATE TABLE IF NOT EXISTS t_component_source (
    component_digest BYTEA   NOT NULL,
    frame_key        TEXT    NOT NULL,
    is_suffix        BOOLEAN NOT NULL,
    digest           TEXT    NOT NULL,
    PRIMARY KEY (component_digest, frame_key, is_suffix),
    FOREIGN KEY (digest) REFERENCES t_file(digest)
);
