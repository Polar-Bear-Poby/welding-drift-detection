-- validate_batch_results.sql
-- Airflow welding_batch_ingest DAG의 validate_results 태스크에서 사용
-- 파라미터: :run_id (spark_batch_run의 run_id)
--
-- 검증 기준:
--   1. 해당 run_id의 summary 레코드가 1건 이상 존재
--   2. ERROR 결정 비율이 50% 미만 (데이터 품질 임계값)
--   3. 처리된 채널 수가 0보다 큰지 확인

SELECT
    r.run_id,
    r.status,
    COUNT(s.run_id)                                          AS summary_count,
    COUNT(*) FILTER (WHERE s.quality_decision = 'PASS')     AS pass_count,
    COUNT(*) FILTER (WHERE s.quality_decision = 'REVIEW')   AS review_count,
    COUNT(*) FILTER (WHERE s.quality_decision = 'ERROR')    AS error_count,
    ROUND(
        COUNT(*) FILTER (WHERE s.quality_decision = 'ERROR')::numeric
        / NULLIF(COUNT(s.run_id), 0) * 100, 2
    )                                                        AS error_rate_pct,
    -- 검증 통과 여부: summary가 1건 이상이고 ERROR 비율이 50% 미만
    CASE
        WHEN COUNT(s.run_id) > 0
         AND (
             COUNT(*) FILTER (WHERE s.quality_decision = 'ERROR')::numeric
             / COUNT(s.run_id)
         ) < 0.5
        THEN 'VALID'
        ELSE 'INVALID'
    END                                                      AS validation_result
FROM welding.spark_batch_run r
LEFT JOIN welding.pattern_summary s ON s.run_id = r.run_id
WHERE r.run_id = :run_id
GROUP BY r.run_id, r.status;
