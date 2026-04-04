DROP TABLE PendingMigrations;

CREATE TABLE InFlightMigrations
(
    source_upstream TEXT NOT NULL,
    target_upstream TEXT NOT NULL,
    bucket          TEXT NOT NULL,
    key             TEXT NOT NULL,
    state           TEXT NOT NULL CHECK (state IN ('Started', 'CopiedToTarget')),

    PRIMARY KEY (source_upstream, bucket, key)
)