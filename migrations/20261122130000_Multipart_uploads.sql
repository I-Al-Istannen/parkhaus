-- Multipart uploads in progress, so every step reaches the upstream the upload was created on.
CREATE TABLE MultipartUploads
(
    upload_id TEXT    NOT NULL PRIMARY KEY,
    bucket    TEXT    NOT NULL,
    key       TEXT    NOT NULL,
    upstream  TEXT    NOT NULL,
    created   INTEGER NOT NULL
);
