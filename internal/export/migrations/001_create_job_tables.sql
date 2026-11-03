-- Processed records. Fields differ per job, so they are stored as a JSON object
-- (query them with json_extract(data, '$.total')).
CREATE TABLE job_records (
    id           INTEGER PRIMARY KEY,
    job_id       TEXT    NOT NULL,
    source       TEXT    NOT NULL,
    record_index INTEGER NOT NULL,
    data         TEXT    NOT NULL
);
CREATE INDEX idx_job_records_job ON job_records (job_id);

-- One row per aggregation group; key and metrics are JSON objects.
CREATE TABLE job_aggregates (
    id          INTEGER PRIMARY KEY,
    job_id      TEXT NOT NULL,
    aggregation TEXT NOT NULL,
    group_key   TEXT NOT NULL,
    metrics     TEXT NOT NULL
);
CREATE INDEX idx_job_aggregates_job ON job_aggregates (job_id, aggregation);
