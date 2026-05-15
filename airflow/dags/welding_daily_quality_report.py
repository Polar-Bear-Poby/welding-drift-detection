"""
welding_daily_quality_report.py - Airflow 3 Version (수정)
==========================================================
매일 01:00에 전일(ds - 1일) 용접 품질 데이터를 집계한다.

수정 내역 (2026-04-28):
  - [fix #3/B] 맨 앞에 @task.short_circuit 추가:
               전일 데이터가 MIN_EXPECTED_ROWS 미만이면 하위 Task 전체 스킵.
               → welding_data_availability_check guard DAG와 schedule이 각자
                 독립적으로 실행되어도 빈 리포트 방지.
  - [fix #4]   aggregate_stats의 집계 기준을 ds → (ds - 1일) = target_date로 변경.
               daily_report는 "실행일(ds)의 전일 데이터"를 기록해야 함.
               (00:30 guard 검사 → 01:00 daily 집계 모두 동일한 target_date 기준)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# ── DB 연결: 환경변수 우선 ────────────────────────────────────────────────
_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB   = os.getenv("POSTGRES_DB",   "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_CONN_STR = f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} user={_PG_USER} password={_PG_PASS}"

DRIFT_THRESHOLD = 0.05
# [fix #3/B] 데이터 최소 행 수 — 라인 3개 × 채널 2개
MIN_EXPECTED_ROWS = 6
DAILY_REPORT_CATCHUP = os.getenv("DAILY_REPORT_CATCHUP", "false").strip().lower() in {"1", "true", "yes", "on"}
DAILY_REPORT_MAX_ACTIVE_RUNS = int(os.getenv("DAILY_REPORT_MAX_ACTIVE_RUNS", "1"))


@dag(
    dag_id="welding_daily_quality_report",
    schedule="0 1 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=DAILY_REPORT_CATCHUP,
    max_active_runs=DAILY_REPORT_MAX_ACTIVE_RUNS,
    tags=["welding", "airflow3", "report"],
    default_args={"owner": "welding-team", "retries": 3},
    doc_md="""
## welding_daily_quality_report

**목적**: 매일 01:00에 **전일(ds - 1일)** 용접 데이터를 집계하고 드리프트를 탐지한다.

**날짜 기준 (fix #4)**
- `ds` = DAG 실행일 (e.g. 2026-04-28)
- `target_date` = ds - 1일 (e.g. 2026-04-27) → 실제 집계 대상

**데이터 가드 (fix #3/B)**
- `check_data_exists`: target_date의 데이터가 MIN_EXPECTED_ROWS 미만이면 short_circuit으로 전체 스킵.
- `welding_data_availability_check` guard DAG와 이중 방어.
    """,
)
def welding_daily_report_dag():

    # [fix #3/B] short_circuit Guard: 전일 데이터 없으면 즉시 스킵
    @task.short_circuit()
    def check_data_exists(ds=None) -> bool:
        """
        target_date(= ds - 1일)의 pattern_summary 행 수가
        MIN_EXPECTED_ROWS 이상인지 확인한다.
        미만이면 False 반환 → 하위 Task 전체 스킵.
        """
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM welding.pattern_summary WHERE event_date = %s",
                    (target_date,),
                )
                count = cur.fetchone()[0]

        log.info(
            "전일(%s) pattern_summary 행 수: %d (최소 요구: %d)",
            target_date, count, MIN_EXPECTED_ROWS,
        )
        if count < MIN_EXPECTED_ROWS:
            log.warning(
                "데이터 부족(%d < %d) → daily_report 스킵. "
                "welding_batch_backfill DAG로 소급 처리 권장.",
                count, MIN_EXPECTED_ROWS,
            )
            return False
        return True

    @task()
    def aggregate_stats(ds=None):
        """
        [fix #4] 집계 기준: ds → target_date (= ds - 1일)
        Idempotent: DELETE + INSERT.
        """
        # target_date = 전일
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM welding.daily_report WHERE report_date = %s",
                    (target_date,),
                )
                cur.execute(
                    """
                    INSERT INTO welding.daily_report (
                        report_date, line_id, channel, total_products, pass_count, review_count,
                        error_count, avg_cpd_score, max_cpd_score, cpd_score_delta, generated_at
                    )
                    WITH today AS (
                        SELECT event_date, line_id, channel, COUNT(*) as total,
                               -- [fix] 정상: 배치 'normal' + 스트리밍 레거시 'PASS' 모두 포함
                               COUNT(*) FILTER (WHERE quality_decision IN ('PASS', 'normal')) as pass,
                               -- review 개념 없음, 0으로 고정
                               0                                                              as review,
                               -- [fix] drift: 새 체계 'drift' 값만
                               COUNT(*) FILTER (WHERE quality_decision = 'drift')             as error,
                               AVG(cpd_score) as avg_cpd, MAX(cpd_score) as max_cpd
                        FROM welding.pattern_summary
                        WHERE event_date = %s
                        GROUP BY event_date, line_id, channel
                    ),
                    yesterday AS (
                        SELECT line_id, channel, AVG(cpd_score) as avg_cpd
                        FROM welding.pattern_summary
                        WHERE event_date = %s::date - INTERVAL '1 day'
                        GROUP BY line_id, channel
                    )
                    SELECT t.event_date, t.line_id, t.channel, t.total, t.pass, t.review, t.error,
                           t.avg_cpd, t.max_cpd,
                           ROUND((t.avg_cpd - COALESCE(y.avg_cpd, t.avg_cpd))::numeric, 6),
                           NOW()
                    FROM today t
                    LEFT JOIN yesterday y ON t.line_id = y.line_id AND t.channel = y.channel
                    """,
                    (target_date, target_date),
                )

                cur.execute(
                    "SELECT COUNT(*) FROM welding.daily_report WHERE report_date = %s",
                    (target_date,),
                )
                count = cur.fetchone()[0]

        log.info("집계 완료: %d 행 (target_date=%s)", count, target_date)
        return count

    @task()
    def detect_drift(count: int, ds=None):
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        if count == 0:
            return

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT line_id, channel, cpd_score_delta FROM welding.daily_report "
                    "WHERE report_date = %s AND ABS(cpd_score_delta) > %s",
                    (target_date, DRIFT_THRESHOLD),
                )
                alerts = cur.fetchall()
                status = "drift_alert" if alerts else "normal"
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.daily_quality_report",
                        f'{{"status":"{status}","date":"{target_date}"}}',
                    ),
                )
        if alerts:
            log.warning("드리프트 경보: %d개 항목 (date=%s)", len(alerts), target_date)

    @task()
    def write_log(ds=None):
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT SUM(total_products) FROM welding.daily_report WHERE report_date = %s",
                    (target_date,),
                )
                total = cur.fetchone()[0] or 0
        log.info("Daily Report [%s]: 총 처리 제품 수 = %d", target_date, total)

    # ── 의존성 연결 ──────────────────────────────────────────────
    guard = check_data_exists()
    c = aggregate_stats()
    guard >> c
    detect_drift(c) >> write_log()


welding_daily_report_dag()


