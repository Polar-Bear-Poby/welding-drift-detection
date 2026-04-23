-- Bootstrap SQL executed once when PostgreSQL data volume is first created.
-- Keep this file idempotent because local environments are frequently reset.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS welding;

CREATE TABLE IF NOT EXISTS welding.pipeline_heartbeat (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    component_name TEXT NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    details JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS welding.spark_batch_run (
    run_id UUID PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    total_files INTEGER NOT NULL DEFAULT 0,
    total_segment_rows INTEGER NOT NULL DEFAULT 0,
    total_summary_rows INTEGER NOT NULL DEFAULT 0,
    output_dir TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS welding.pattern_segment (
    run_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    channel SMALLINT NOT NULL,
    segment_index SMALLINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    line_id TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    parity_group TEXT NOT NULL,
    parity_order SMALLINT NOT NULL,
    sample_count INTEGER NOT NULL,
    mean_value DOUBLE PRECISION,
    std_value DOUBLE PRECISION,
    min_value DOUBLE PRECISION,
    max_value DOUBLE PRECISION,
    PRIMARY KEY (run_id, source_file, channel, segment_index)
);

CREATE TABLE IF NOT EXISTS welding.pattern_summary (
    run_id UUID NOT NULL,
    source_file TEXT NOT NULL,
    channel SMALLINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    event_date DATE NOT NULL,
    line_id TEXT NOT NULL,
    line_number INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    record_count INTEGER NOT NULL,
    total_samples INTEGER NOT NULL,
    odd_pattern_mean DOUBLE PRECISION,
    even_pattern_mean DOUBLE PRECISION,
    odd_even_gap DOUBLE PRECISION,
    cpd_score DOUBLE PRECISION,
    quality_decision TEXT NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    PRIMARY KEY (run_id, source_file, channel)
);

CREATE INDEX IF NOT EXISTS idx_pattern_segment_event
    ON welding.pattern_segment (event_date, line_number, product_id);

CREATE INDEX IF NOT EXISTS idx_pattern_summary_event
    ON welding.pattern_summary (event_date, line_number, quality_decision);
