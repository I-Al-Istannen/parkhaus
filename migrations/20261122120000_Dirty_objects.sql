-- An object a mutation or migration is changing, until the outcome is known. `target` is the
-- upstream that should hold the object afterwards, `previous` the one that held it before.
-- Migrations move an object without changing it, so resolving them keeps its timestamps.
CREATE TABLE DirtyObjects
(
    bucket    TEXT    NOT NULL,
    key       TEXT    NOT NULL,
    target    TEXT    NOT NULL,
    previous  TEXT,
    migration BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (bucket, key)
);

INSERT OR IGNORE INTO DirtyObjects (bucket, key, target, previous, migration)
SELECT i.bucket, i.key, i.target_upstream, i.source_upstream, TRUE
FROM InFlightMigrations i
         JOIN Objects o ON o.bucket = i.bucket AND o.key = i.key;

DROP TABLE InFlightMigrations;
