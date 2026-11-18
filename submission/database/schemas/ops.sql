CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    pipeline_run_id TEXT PRIMARY KEY,

    pipeline_name TEXT NOT NULL DEFAULT 'omnichannel_retail',

    status TEXT NOT NULL DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),

    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    finished_at TIMESTAMPTZ,

    error_message TEXT,

    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE TABLE IF NOT EXISTS ops.ingested_files (
    file_id BIGSERIAL PRIMARY KEY,

    pipeline_run_id TEXT NOT NULL
        REFERENCES ops.pipeline_runs(pipeline_run_id),

    source_file TEXT NOT NULL,

    file_checksum TEXT NOT NULL
        CHECK (file_checksum ~ '^[0-9a-f]{64}$'),

    file_size_bytes BIGINT NOT NULL
        CHECK (file_size_bytes >= 0),

    record_count BIGINT NOT NULL
        CHECK (record_count >= 0),

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (source_file, file_checksum)
);

CREATE TABLE IF NOT EXISTS ops.quality_results (
    quality_result_id BIGSERIAL PRIMARY KEY,

    pipeline_run_id TEXT NOT NULL
        REFERENCES ops.pipeline_runs(pipeline_run_id),

    stage TEXT NOT NULL,

    check_name TEXT NOT NULL,

    status TEXT NOT NULL
        CHECK (status IN ('PASS', 'FAIL', 'WARN')),

    failed_record_count BIGINT NOT NULL DEFAULT 0
        CHECK (failed_record_count >= 0),

    details JSONB NOT NULL DEFAULT '{}'::JSONB,

    checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (pipeline_run_id, stage, check_name)
);

-- Waktu pemuatan awal boleh kosong jika tidak diketahui.
ALTER TABLE ops.ingested_files
    ALTER COLUMN loaded_at DROP NOT NULL;

-- Waktu pendaftaran informasi file ke Ops.
ALTER TABLE ops.ingested_files
    ADD COLUMN IF NOT EXISTS registered_at
        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP;