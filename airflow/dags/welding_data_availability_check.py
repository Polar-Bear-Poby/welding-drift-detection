"""
welding_data_availability_check.py - Airflow 3 Version (수정)
=============================================================
매일 00:30에 전일(target_date = ds - 1일) 데이터가 pattern_summary 테이블에
적재되어 있는지 확인하고, 없으면 alert을 기록한다.

수정 내역 (2026-04-28):
  - [fix #3] daily DAG 중복 실행 방지:
             guard는 트리거를 수행하지 않고, 선행 가용성 체크/알림 역할만 담당.
             실제 실행 제어는 daily DAG 내부 short_circuit Guard가 담당.
  - [fix #4] 날짜 기준: yesterday → target_date(= ds - 1일)로 명시적 변수화.
             daily DAG의 집계 기준(target_date)과 완전히 일치.
  - [fix #7] DB_CONN_STR 환경변수 기반으로 변경.

DAG 흐름:
    check_data_exists_for_yesterday
        ├── [데이터 있음] → log_data_ready
        └── [데이터 없음] → alert_missing_data → mark_skip_report
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

# [fix #7] 환경변수 기반 DB 연결
_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB   = os.getenv("POSTGRES_DB",   "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} user={_PG_USER} password={_PG_PASS}"

# 최소 기대 행 수 (라인 3개 × 채널 2개 = 6개 이상이면 정상)
MIN_EXPECTED_ROWS = 6


@dag(
    dag_id="welding_data_availability_check",
    schedule="30 0 * * *",  # 매일 00:30
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "guard", "data-quality"],
    default_args={
        "owner": "welding-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md="""
## welding_data_availability_check

**목적**: `welding_daily_quality_report`(01:00) 실행 전에 전일 데이터가
`welding.pattern_summary`에 충분히 적재되어 있는지 사전 검증한다.

**임계값**: 최소 {} 행 이상 존재해야 정상 판정.

**DAG 흐름**
```
    check_data_exists_for_yesterday
        ├── [정상] → log_data_ready
        └── [없음] → alert_missing_data → mark_skip_report
```
    """.format(MIN_EXPECTED_ROWS),
)
def welding_data_availability_check_dag():

    @task.branch()
    def check_data_exists_for_yesterday(ds=None):
        """
        [fix #4] target_date = ds - 1일: daily_report DAG의 집계 기준과 동일.
        MIN_EXPECTED_ROWS 이상이면 데이터 준비 완료로 기록, 미만이면 alert.
        """
        # target_date = 전일 (daily DAG의 집계 기준과 동일하게 맞춤)
        target_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*), COUNT(DISTINCT line_id), COUNT(DISTINCT channel) "
                        "FROM welding.pattern_summary "
                        "WHERE event_date = %s",
                        (target_date,),
                    )
                    row_count, line_count, channel_count = cur.fetchone()

            log.info(
                "target_date(%s) 데이터 확인 — 행: %d, 라인: %d, 채널: %d",
                target_date, row_count, line_count, channel_count,
            )

            # heartbeat 기록
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                        "VALUES (%s, %s::jsonb)",
                        (
                            "airflow.data_availability_check",
                            json.dumps(
                                {
                                    "target_date": target_date,
                                    "row_count": row_count,
                                    "line_count": line_count,
                                    "channel_count": channel_count,
                                }
                            ),
                        ),
                    )

            if row_count >= MIN_EXPECTED_ROWS:
                return "log_data_ready"
            else:
                log.warning(
                    "target_date(%s) 데이터 부족: %d 행 < 최소 %d 행",
                    target_date, row_count, MIN_EXPECTED_ROWS,
                )
                return "alert_missing_data"

        except Exception as exc:
            log.error("데이터 가용성 확인 중 오류: %s", exc)
            return "alert_missing_data"

    @task()
    def log_data_ready(ds=None):
        """daily_report 실행 전제 충족(전일 데이터 준비 완료) 상태를 기록."""
        target_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.data_availability_check",
                        json.dumps(
                            {
                                "status": "data_ready",
                                "target_date": target_date,
                                "note": "welding_daily_quality_report is scheduled independently at 01:00",
                            }
                        ),
                    ),
                )
        log.info("✅ target_date=%s 데이터 준비 완료", target_date)

    @task()
    def alert_missing_data(ds=None):
        """target_date 데이터 부족 시 DB에 alert 기록."""
        target_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        log.error("❌ [DATA MISSING] %s 날짜의 용접 데이터가 부족합니다!", target_date)
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.data_availability_check",
                        json.dumps({
                            "status": "data_missing",
                            "missing_date": target_date,
                            "action": "daily_report_skipped",
                        }),
                    ),
                )

    @task()
    def mark_skip_report(ds=None):
        """daily_report 스킵 기록 + backfill 권장 메시지."""
        target_date = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.data_availability_check",
                        json.dumps({
                            "status": "report_skipped",
                            "skipped_date": target_date,
                            "recommendation": f"Run welding_batch_backfill with target_date={target_date.replace('-', '')}",
                        }),
                    ),
                )
        log.warning(
            "⚠️ %s 날짜 daily_report 건너뜀. welding_batch_backfill DAG로 소급 처리 권장.",
            target_date,
        )

    # ── 의존성 연결 ──────────────────────────────────────────────
    branch = check_data_exists_for_yesterday()
    branch >> log_data_ready()
    branch >> alert_missing_data() >> mark_skip_report()


welding_data_availability_check_dag()
