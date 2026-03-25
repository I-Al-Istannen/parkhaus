CREATE TABLE Objects
(
    bucket            TEXT    NOT NULL,
    key               TEXT    NOT NULL,
    assigned_upstream TEXT    NOT NULL,
    last_modified     INTEGER NOT NULL,

    PRIMARY KEY (bucket, key)
);

CREATE TABLE PendingMigrations
(
    source_upstream TEXT NOT NULL,
    target_upstream TEXT NOT NULL,
    bucket          TEXT NOT NULL,
    key             TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('Pending', 'CopiedToTarget', 'Finished')),

    PRIMARY KEY (source_upstream, bucket, key)
);