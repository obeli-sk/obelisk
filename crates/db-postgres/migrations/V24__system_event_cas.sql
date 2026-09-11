ALTER TABLE t_system_event ADD COLUMN cas_digest TEXT REFERENCES t_file(digest);
