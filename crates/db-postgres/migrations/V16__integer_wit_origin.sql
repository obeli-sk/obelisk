-- backcompat: migrate the textual WIT origins written through Obelisk 0.40.x.
ALTER TABLE t_component_metadata
ALTER COLUMN wit_origin TYPE SMALLINT
USING CASE wit_origin
    WHEN 'wasm' THEN 1
    WHEN 'synthesized' THEN 2
END;

ALTER TABLE t_component_metadata
ADD CONSTRAINT t_component_metadata_wit_origin_check CHECK (wit_origin IN (1, 2));
