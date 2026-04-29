"""
welding_weekly_drift_trend.py - Airflow 3 Version
==================================================
매주 월요일 09:00에 지난 7일간의 daily_report 데이터를 집계하여
장기 드리프트 트렌드를 분석하고 weekly_trend 테이블에 저장한다.

단기 spike가 아닌 지속적 상승 트렌드(7일 이동평균)를 감지하는 것이 목표.

DAG 흐름:
    check_weekly_data_ready
        └── [데이터 충분] → aggregate_weekly_stats → detect_long_term_drift → write_weekly_summary
        └── [데이터 부족] → alert_insufficient_data

사전 조건:
    weekly_trend 테이블이 없으면 이 DAG가 자동 생성한다.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)

# 7일 중 최소 몇 일치 데이터가 있어야 주간 분석 수행하는가
MIN_DAYS_REQUIRED = 5

# cpd_score 7일 이동평균이 이 값 이상으로 상승하면 장기 드리프트 경보
LONG_TERM_DRIFT_THRESHOLD = 0.03

# weekly_trend DDL (자동 생성)
CREATE_WEEKLY_TREND_TABLE = """
CREATE TABLE IF NOT EXISTS welding.weekly_trend (
    week_start      DATE            NOT NULL,
    week_end        DATE            NOT NULL,
    line_id         TEXT            NOT NULL,
    channel         SMALLINT        NOT NULL,
    days_with_data  INTEGER         NOT NULL DEFAULT 0,
    total_products  INTEGER         NOT NULL DEFAULT 0,
    avg_cpd_score   DOUBLE PRECISION,
    max_cpd_score   DOUBLE PRECISION,
    cpd_trend_slope DOUBLE PRECISION,   -- 회귀 기울기 (양수=악화 추세)
    long_term_drift BOOLEAN         NOT NULL DEFAULT FALSE,
    pass_rate       DOUBLE PRECISION,
    generated_at    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    PRIMARY KEY (week_start, line_id, channel)
);
CREATE INDEX IF NOT EXISTS idx_weekly_trend_week
    ON welding.weekly_trend (week_start DESC);
"""


@dag(
    dag_id="welding_weekly_drift_trend",
    schedule="0 9 * * MON",  # 매주 월요일 09:00
    start_date=datetime(2026, 4, 7),  # 첫 번째 월요일
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "trend", "weekly"],
    default_args={
        "owner": "welding-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md="""
## welding_weekly_drift_trend

**목적**: 지난 7일간의 `daily_report` 데이터를 분석하여 장기 드리프트 트렌드를 감지한다.
단기 spike가 아닌 지속적 상승 추세를 포착하는 것이 핵심.

**장기 드리프트 판정 기준**:
- 7일 평균 cpd_score > `LONG_TERM_DRIFT_THRESHOLD` (현재: {threshold})
- 또는 선형 회귀 기울기(slope) > 0 (지속 상승 추세)

**결과 테이블**: `welding.weekly_trend`
    """.format(threshold=LONG_TERM_DRIFT_THRESHOLD),
)
def welding_weekly_drift_trend_dag():

    @task()
    def ensure_table_exists():
        """weekly_trend 테이블이 없으면 생성한다."""
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_WEEKLY_TREND_TABLE)
        log.info("welding.weekly_trend 테이블 확인/생성 완료")

    @task.branch()
    def check_weekly_data_ready(ds=None):
        """
        지난 7일(월~일) 동안 daily_report에 데이터가 있는 날수를 확인한다.
        MIN_DAYS_REQUIRED 미만이면 분석 스킵.
        """
        # ds는 월요일 날짜 → 지난 7일 = ds-7 ~ ds-1
        week_end = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        week_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(DISTINCT report_date) FROM welding.daily_report "
                    "WHERE report_date BETWEEN %s AND %s",
                    (week_start, week_end),
                )
                days_count = cur.fetchone()[0]

        log.info(
            "주간 데이터 확인 (%s ~ %s): %d일치 데이터 (최소 요구: %d일)",
            week_start, week_end, days_count, MIN_DAYS_REQUIRED,
        )

        if days_count >= MIN_DAYS_REQUIRED:
            return "aggregate_weekly_stats"
        else:
            log.warning(
                "데이터 부족(%d/%d일) → 주간 분석 스킵",
                days_count, MIN_DAYS_REQUIRED,
            )
            return "alert_insufficient_data"

    @task()
    def aggregate_weekly_stats(ds=None) -> dict:
        """
        지난 7일의 daily_report를 라인별/채널별로 집계한다.
        선형 회귀로 cpd_score 상승 기울기(slope)를 계산한다.
        """
        week_end = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        week_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                # ── 주간 집계 (DELETE + INSERT 멱등성 보장) ──
                cur.execute(
                    "DELETE FROM welding.weekly_trend WHERE week_start = %s",
                    (week_start,),
                )

                # 선형 회귀 기울기: REGR_SLOPE(y, x)에서 x=날짜 순서(epoch), y=avg_cpd_score
                cur.execute(
                    """
                    INSERT INTO welding.weekly_trend (
                        week_start, week_end, line_id, channel,
                        days_with_data, total_products,
                        avg_cpd_score, max_cpd_score, cpd_trend_slope,
                        long_term_drift, pass_rate, generated_at
                    )
                    SELECT
                        %s::date                                    AS week_start,
                        %s::date                                    AS week_end,
                        line_id,
                        channel,
                        COUNT(DISTINCT report_date)                 AS days_with_data,
                        SUM(total_products)                         AS total_products,
                        AVG(avg_cpd_score)                          AS avg_cpd_score,
                        MAX(max_cpd_score)                          AS max_cpd_score,
                        -- 선형 회귀 기울기: 양수 = cpd_score 상승 추세
                        REGR_SLOPE(avg_cpd_score,
                            EXTRACT(EPOCH FROM report_date::timestamp))
                                                                    AS cpd_trend_slope,
                        -- 장기 드리프트 판정: 주평균 초과 OR 기울기 양수
                        (AVG(avg_cpd_score) > %s
                         OR REGR_SLOPE(avg_cpd_score,
                            EXTRACT(EPOCH FROM report_date::timestamp)) > 0)
                                                                    AS long_term_drift,
                        -- pass 비율
                        CASE WHEN SUM(total_products) > 0
                             THEN SUM(pass_count)::float / SUM(total_products)
                             ELSE NULL END                          AS pass_rate,
                        NOW()                                       AS generated_at
                    FROM welding.daily_report
                    WHERE report_date BETWEEN %s AND %s
                    GROUP BY line_id, channel
                    """,
                    (week_start, week_end, LONG_TERM_DRIFT_THRESHOLD, week_start, week_end),
                )
                inserted = cur.rowcount

                # 집계 결과 조회
                cur.execute(
                    "SELECT line_id, channel, avg_cpd_score, cpd_trend_slope, long_term_drift "
                    "FROM welding.weekly_trend WHERE week_start = %s",
                    (week_start,),
                )
                rows = cur.fetchall()

        log.info("주간 집계 완료: %d 라인-채널 조합, %s ~ %s", inserted, week_start, week_end)
        return {
            "week_start": week_start,
            "week_end": week_end,
            "inserted": inserted,
            "rows": [
                {
                    "line_id": r[0],
                    "channel": r[1],
                    "avg_cpd": round(r[2], 6) if r[2] else None,
                    "slope": round(r[3], 8) if r[3] else None,
                    "long_term_drift": r[4],
                }
                for r in rows
            ],
        }

    @task()
    def detect_long_term_drift(weekly_stats: dict):
        """
        장기 드리프트 감지 결과를 분석하고 heartbeat에 기록한다.
        long_term_drift=True인 라인-채널 조합을 경보로 기록.
        """
        alerts = [r for r in weekly_stats.get("rows", []) if r.get("long_term_drift")]
        status = "long_term_drift_alert" if alerts else "trend_normal"

        if alerts:
            log.warning(
                "🚨 장기 드리프트 감지! %d개 라인-채널 조합: %s",
                len(alerts),
                json.dumps(alerts, ensure_ascii=False),
            )
        else:
            log.info("✅ 주간 드리프트 정상 — 장기 추세 이상 없음")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.weekly_drift_trend",
                        json.dumps(
                            {
                                "status": status,
                                "week_start": weekly_stats["week_start"],
                                "week_end": weekly_stats["week_end"],
                                "drift_alerts": alerts,
                                "total_combinations": weekly_stats.get("inserted", 0),
                            }
                        ),
                    ),
                )
        return status

    @task()
    def write_weekly_summary(drift_status: str, weekly_stats: dict):
        """주간 요약 로그 출력 및 최종 heartbeat 기록."""
        rows = weekly_stats.get("rows", [])
        log.info(
            "📊 주간 드리프트 요약 [%s ~ %s]",
            weekly_stats["week_start"],
            weekly_stats["week_end"],
        )
        for r in rows:
            drift_flag = "⚠️ DRIFT" if r.get("long_term_drift") else "✅ OK"
            log.info(
                "  %s | Line: %-8s Ch: %d | avg_cpd: %.6f | slope: %s",
                drift_flag,
                r["line_id"],
                r["channel"],
                r.get("avg_cpd") or 0.0,
                f"{r['slope']:+.8f}" if r.get("slope") is not None else "N/A",
            )

    @task()
    def alert_insufficient_data(ds=None):
        """주간 데이터가 MIN_DAYS_REQUIRED 미만인 경우 기록."""
        week_end = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        week_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")

        log.warning(
            "⚠️ 주간 분석 스킵: %s ~ %s 데이터 부족 (최소 %d일 필요)",
            week_start, week_end, MIN_DAYS_REQUIRED,
        )
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.weekly_drift_trend",
                        json.dumps(
                            {
                                "status": "insufficient_data",
                                "week_start": week_start,
                                "week_end": week_end,
                                "min_days_required": MIN_DAYS_REQUIRED,
                            }
                        ),
                    ),
                )

    # ── 의존성 연결 ──────────────────────────────────────────────
    table_ready = ensure_table_exists()
    branch = check_weekly_data_ready()
    table_ready >> branch

    stats = aggregate_weekly_stats()
    drift_status = detect_long_term_drift(stats)
    branch >> stats >> drift_status >> write_weekly_summary(drift_status, stats)

    branch >> alert_insufficient_data()


welding_weekly_drift_trend_dag()


