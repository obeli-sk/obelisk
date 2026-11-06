-- Only used by the libSQL backend: commit guard and chunks of files above its value size limit.
CREATE TABLE t_write_epoch (id INTEGER PRIMARY KEY CHECK (id = 0), epoch INTEGER NOT NULL);
INSERT INTO t_write_epoch (id, epoch) VALUES (0, 0);
CREATE TABLE t_file_chunk (
    digest TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    content BLOB NOT NULL,
    PRIMARY KEY (digest, chunk_index)
) STRICT;
