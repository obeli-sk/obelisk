-- backcompat: migrate the textual WIT origins written through Obelisk 0.40.x.
CREATE TEMPORARY TABLE _v16_wit_origin (value INTEGER NOT NULL CHECK (value IN (1, 2)));
INSERT INTO _v16_wit_origin
SELECT CASE wit_origin
    WHEN 'wasm' THEN 1
    WHEN 'synthesized' THEN 2
END
FROM t_component_metadata;
DROP TABLE _v16_wit_origin;

ALTER TABLE t_component_metadata
ADD COLUMN wit_origin_integer INTEGER NOT NULL DEFAULT 1 CHECK (wit_origin_integer IN (1, 2));

UPDATE t_component_metadata
SET wit_origin_integer = CASE wit_origin
    WHEN 'wasm' THEN 1
    WHEN 'synthesized' THEN 2
END;

ALTER TABLE t_component_metadata DROP COLUMN wit_origin;
ALTER TABLE t_component_metadata RENAME COLUMN wit_origin_integer TO wit_origin;
