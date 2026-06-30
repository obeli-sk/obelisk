-- Consolidate backtrace source blobs into the content-addressed store (t_file).
-- t_component_source now references t_file by digest; the standalone
-- t_source_file blob store is removed. Existing source data is discarded;
-- sources are repopulated on the next deployment submission.

DROP TABLE IF EXISTS t_component_source;
DROP TABLE IF EXISTS t_source_file;

-- Maps (component_digest, frame_key, is_suffix) to a blob in t_file.
-- frame_key is the exact frame symbol path (is_suffix=0) or a '/'-prefixed suffix (is_suffix=1).
CREATE TABLE IF NOT EXISTS t_component_source (
    component_digest BLOB    NOT NULL,
    frame_key        TEXT    NOT NULL,
    is_suffix        INTEGER NOT NULL,
    digest           TEXT    NOT NULL,

    PRIMARY KEY (component_digest, frame_key, is_suffix),
    FOREIGN KEY (digest) REFERENCES t_file(digest)
) STRICT;

CREATE TRIGGER t_component_source_bounded_text_insert
BEFORE INSERT ON t_component_source
BEGIN
    SELECT RAISE(ABORT, 't_component_source.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;

CREATE TRIGGER t_component_source_bounded_text_update
BEFORE UPDATE OF digest ON t_component_source
BEGIN
    SELECT RAISE(ABORT, 't_component_source.digest exceeds 71 characters')
    WHERE length(NEW.digest) > 71;
END;
