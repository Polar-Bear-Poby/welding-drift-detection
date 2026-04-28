-- aggregate_daily_stats.sql
-- Airflow welding_daily_quality_report DAG의 aggregate_daily_stats 태스크에서 사용
-- 파라미터: :report_date (YYYY-MM-DD 형식, Airflow의 {{ ds }} 변수)
--
-- Idempotency 보장: 같은 날짜를 다시 실행해도 중복 없이 덮어씀
-- Step 1: 기존 데이터 삭제 (같은 날 재실행 안전성)
DELETE FROM welding.daily_report
WHERE report_date = :report_date;

-- Step 2: 전일 대비 변화율(cpd_score_delta) 포함 집계 삽입
INSERT INTO welding.daily_report (
    report_date,
    line_id,
    channel,
    total_products,
    pass_count,
    review_count,
    error_count,
    avg_cpd_score,
    max_cpd_score,
    cpd_score_delta,
    generated_at
)
WITH today_stats AS (
    -- 오늘 날짜의 라인별/채널별 집계
    SELECT
        event_date,
        line_id,
        channel,
        COUNT(*)                                             AS total_products,
        COUNT(*) FILTER (WHERE quality_decision = 'PASS')   AS pass_count,
        COUNT(*) FILTER (WHERE quality_decision = 'REVIEW') AS review_count,
        COUNT(*) FILTER (WHERE quality_decision = 'ERROR')  AS error_count,
        AVG(cpd_score)                                       AS avg_cpd_score,
        MAX(cpd_score)                                       AS max_cpd_score
    FROM welding.pattern_summary
    WHERE event_date = :report_date
    GROUP BY event_date, line_id, channel
),
yesterday_stats AS (
    -- 전일 평균 cpd_score (드리프트 트렌드 비교용)
    SELECT
        line_id,
        channel,
        AVG(cpd_score) AS avg_cpd_score
    FROM welding.pattern_summary
    WHERE event_date = :report_date::date - INTERVAL '1 day'
    GROUP BY line_id, channel
)
SELECT
    t.event_date                                        AS report_date,
    t.line_id,
    t.channel,
    t.total_products,
    t.pass_count,
    t.review_count,
    t.error_count,
    t.avg_cpd_score,
    t.max_cpd_score,
    -- 전일 대비 변화율: 양수면 드리프트 악화, 음수면 개선
    ROUND(
        (t.avg_cpd_score - COALESCE(y.avg_cpd_score, t.avg_cpd_score))::numeric,
        6
    )                                                   AS cpd_score_delta,
    NOW()                                               AS generated_at
FROM today_stats t
LEFT JOIN yesterday_stats y
    ON t.line_id = y.line_id AND t.channel = y.channel;
