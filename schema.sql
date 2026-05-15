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
    model_name TEXT NOT NULL DEFAULT '',
    model_version TEXT NOT NULL DEFAULT '',
    inference_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    segment_drift_flag BOOLEAN NOT NULL DEFAULT FALSE,
    inference_ms INTEGER NOT NULL DEFAULT 0,
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
    drift_segment_count INTEGER NOT NULL DEFAULT 0,
    drift_segment_ratio DOUBLE PRECISION NOT NULL DEFAULT 0,
    quality_decision TEXT NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    PRIMARY KEY (run_id, source_file, channel)
);

CREATE TABLE IF NOT EXISTS welding.reassembly_audit (
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    batch_id BIGINT NOT NULL,
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    product_instance_id TEXT NOT NULL,
    product_id TEXT,
    line_id TEXT NOT NULL,
    lead_num INTEGER NOT NULL,
    channel SMALLINT NOT NULL,
    replay_iteration INTEGER NOT NULL DEFAULT 0,
    expected_chunks INTEGER NOT NULL DEFAULT 0,
    received_chunks INTEGER NOT NULL DEFAULT 0,
    unique_chunk_indexes INTEGER NOT NULL DEFAULT 0,
    total_chunks_variants INTEGER NOT NULL DEFAULT 0,
    min_chunk_index INTEGER,
    max_chunk_index INTEGER,
    expected_samples INTEGER,
    reassembled_samples INTEGER,
    reassembly_status TEXT NOT NULL,
    status_reason TEXT,
    PRIMARY KEY (
        batch_id,
        product_instance_id,
        line_id,
        lead_num,
        channel,
        replay_iteration
    )
);

CREATE INDEX IF NOT EXISTS idx_pattern_segment_event
    ON welding.pattern_segment (event_date, line_number, product_id);

CREATE INDEX IF NOT EXISTS idx_pattern_summary_event
    ON welding.pattern_summary (event_date, line_number, quality_decision);

CREATE INDEX IF NOT EXISTS idx_reassembly_audit_observed
    ON welding.reassembly_audit (observed_at DESC, reassembly_status);

-- Airflow daily_quality_report DAG가 매일 집계한 라인별 품질 트렌드를 저장
CREATE TABLE IF NOT EXISTS welding.daily_report (
    report_date     DATE        NOT NULL,
    line_id         TEXT        NOT NULL,
    channel         SMALLINT    NOT NULL,
    total_products  INTEGER     NOT NULL DEFAULT 0,
    pass_count      INTEGER     NOT NULL DEFAULT 0,
    review_count    INTEGER     NOT NULL DEFAULT 0,
    error_count     INTEGER     NOT NULL DEFAULT 0,
    avg_cpd_score   DOUBLE PRECISION,
    max_cpd_score   DOUBLE PRECISION,
    -- 전일 대비 cpd_score 변화율 (drift trend 핵심 지표)
    cpd_score_delta DOUBLE PRECISION,
    generated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_date, line_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_daily_report_date
    ON welding.daily_report (report_date DESC);

-- welding_weekly_drift_trend DAG가 매주 월요일 집계한 라인별 장기 드리프트 트렌드
CREATE TABLE IF NOT EXISTS welding.weekly_trend (
    week_start      DATE            NOT NULL,
    week_end        DATE            NOT NULL,
    line_id         TEXT            NOT NULL,
    channel         SMALLINT        NOT NULL,
    days_with_data  INTEGER         NOT NULL DEFAULT 0,
    total_products  INTEGER         NOT NULL DEFAULT 0,
    avg_cpd_score   DOUBLE PRECISION,
    max_cpd_score   DOUBLE PRECISION,
    -- 선형 회귀 기울기: 양수=악화 추세, 음수=개선 추세
    cpd_trend_slope DOUBLE PRECISION,
    -- 장기 드리프트 판정 플래그
    long_term_drift BOOLEAN         NOT NULL DEFAULT FALSE,
    pass_rate       DOUBLE PRECISION,
    generated_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (week_start, line_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_weekly_trend_week
    ON welding.weekly_trend (week_start DESC);

CREATE TABLE IF NOT EXISTS welding.stage_event (
    run_id UUID NOT NULL,
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    ended_at TIMESTAMPTZ,
    input_hash TEXT,
    error_code TEXT,
    error_message TEXT,
    detail_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, stage_name)
);
