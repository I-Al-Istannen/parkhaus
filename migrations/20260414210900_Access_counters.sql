CREATE TABLE AccessCounters
(
    obj_bucket  TEXT    NOT NULL,
    obj_key     TEXT    NOT NULL,
    time_bucket INTEGER NOT NULL,
    count       INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (obj_bucket, obj_key, time_bucket),
    FOREIGN KEY (obj_bucket, obj_key) REFERENCES Objects (bucket, key) ON DELETE CASCADE
);

ALTER TABLE Objects
    ADD COLUMN last_accessed INTEGER NOT NULL DEFAULT 0;
-- noinspection SqlWithoutWhere
UPDATE Objects SET last_accessed = last_modified;