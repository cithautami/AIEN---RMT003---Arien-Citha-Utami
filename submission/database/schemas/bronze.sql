CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_records (
    source_file TEXT NOT NULL,

    source_row_number INTEGER NOT NULL,

    source_record_id TEXT,

    file_sha256 TEXT NOT NULL,

    payload JSONB NOT NULL,

    pipeline_run_id TEXT NOT NULL,

    PRIMARY KEY (
        source_file,
        file_sha256,
        source_row_number
    )
);