CREATE TABLE objects
(
    bucket            TEXT    NOT NULL,
    key               TEXT    NOT NULL,
    assigned_upstream TEXT    NOT NULL,
    last_modified     INTEGER NOT NULL,

    PRIMARY KEY (bucket, key)
);