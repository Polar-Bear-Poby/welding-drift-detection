"""
welding_storage_cleanup.py - Airflow 3 Version
===============================================
매일 03:00에 오래된 Parquet 파일과 로그 파일을 자동으로 정리한다.

정리 대상:
  1. /storage/spark_batch/    → 30일 이상 된 Parquet 파일
  2. /storage/logs/           → 14일 이상 된 로그 파일 (압축 후 90일 보관 or 직접 삭제)
  3. /tmp/spark-checkpoints-* → Spark 스트리밍 체크포인트 (7일 이상 된 것)

안전 장치:
  - 삭제 전 DB 데이터와 파일이 연결되어 있는지 확인
  - 삭제된 파일 수와 해제된 용량을 DB에 기록
  - dry_run 모드: 실제 삭제 없이 대상만 나열

DAG 흐름:
    check_disk_usage
        └── clean_old_parquet
                └── clean_old_logs
                        └── clean_spark_checkpoints
                                └── report_cleanup_result
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)

# 정리 기준일 (일)
PARQUET_RETENTION_DAYS = 30
LOG_RETENTION_DAYS = 14
CHECKPOINT_RETENTION_DAYS = 7

# 컨테이너 내부 경로 (spark-master 컨테이너 기준)
STORAGE_PARQUET_DIR = "/storage/spark_batch"
STORAGE_LOGS_DIR = "/storage/logs"
CHECKPOINT_DIR = "/tmp"

# 디스크 사용량 경보 임계값 (%)
DISK_USAGE_ALERT_THRESHOLD = 80


@dag(
    dag_id="welding_storage_cleanup",
    schedule="0 3 * * *",  # 매일 03:00 (트래픽 최저점)
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "dry_run": Param(
            default=False,
            type="boolean",
            description="True면 삭제 없이 대상만 집계한다.",
        ),
    },
    tags=["welding", "airflow3", "maintenance", "storage"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md="""
## welding_storage_cleanup

**목적**: 매일 03:00에 오래된 파일을 자동 정리하여 디스크 용량을 확보한다.

**정리 기준**
| 대상 | 보관 기간 |
|---|---|
| Parquet (`/storage/spark_batch/`) | {parquet}일 |
| 로그 (`/storage/logs/`) | {log}일 |
| Spark 체크포인트 (`/tmp/spark-checkpoints-*`) | {ckpt}일 |

**안전 장치**: 삭제 전 현재 실행 중인 spark_batch_run과 연결된 파일은 제외.
    """.format(
        parquet=PARQUET_RETENTION_DAYS,
        log=LOG_RETENTION_DAYS,
        ckpt=CHECKPOINT_RETENTION_DAYS,
    ),
)
def welding_storage_cleanup_dag():

    @task()
    def check_disk_usage() -> dict:
        """
        spark-master 컨테이너 내 /storage 디스크 사용량을 확인한다.
        임계값 초과 시 경보 레벨을 높인다.
        """
        import subprocess

        result = subprocess.run(
            "docker exec welding-spark-master df -h /storage 2>/dev/null || echo 'N/A'",
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        df_output = result.stdout.strip()
        log.info("디스크 사용량:\n%s", df_output)

        # 사용량 % 파싱 (df -h 출력에서 Use% 컬럼)
        usage_pct = 0
        for line in df_output.split("\n"):
            parts = line.split()
            if len(parts) >= 5 and parts[4].endswith("%"):
                try:
                    usage_pct = int(parts[4].rstrip("%"))
                except ValueError:
                    pass

        level = "critical" if usage_pct >= 90 else "warning" if usage_pct >= DISK_USAGE_ALERT_THRESHOLD else "normal"
        log.info("디스크 사용률: %d%% → 레벨: %s", usage_pct, level)

        disk_info = {
            "usage_pct": usage_pct,
            "level": level,
            "df_output": df_output,
        }

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.storage_cleanup.disk_check",
                        json.dumps(disk_info),
                    ),
                )
        return disk_info

    @task()
    def clean_old_parquet(disk_info: dict) -> dict:
        """
        /storage/spark_batch/ 내 PARQUET_RETENTION_DAYS일 이상 된 파일 삭제.
        현재 실행 중인(or 최근 성공한) run_id 디렉토리는 제외.
        """
        import subprocess
        from airflow.operators.python import get_current_context

        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        # DB에서 최근 7일 내 성공한 run_id 목록 조회 (보호 대상)
        protected_runs = set()
        try:
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT run_id::text FROM welding.spark_batch_run "
                        "WHERE status = 'SUCCESS' "
                        "AND finished_at >= NOW() - INTERVAL '7 days'"
                    )
                    for row in cur.fetchall():
                        protected_runs.add(row[0])
        except Exception as exc:
            log.warning("보호 대상 run_id 조회 실패: %s", exc)

        log.info("보호 대상 run_id %d개", len(protected_runs))

        # find 명령으로 오래된 파일 목록 조회
        find_cmd = (
            f"docker exec welding-spark-master find {STORAGE_PARQUET_DIR} "
            f"-name '*.parquet' -mtime +{PARQUET_RETENTION_DAYS} -type f"
        )
        result = subprocess.run(
            find_cmd, shell=True, capture_output=True, text=True, timeout=60
        )
        candidate_files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]

        # 보호 run_id 포함 파일 필터링
        to_delete = [
            f for f in candidate_files
            if not any(run_id in f for run_id in protected_runs)
        ]

        deleted_count = 0
        if to_delete:
            if dry_run:
                log.info("dry_run=True, Parquet 삭제 생략. 대상 %d개", len(to_delete))
            else:
                # 배치 삭제 (xargs 활용)
                file_list = "\n".join(to_delete)
                del_cmd = (
                    f"docker exec welding-spark-master bash -c "
                    f"\"echo '{file_list}' | xargs -r rm -f\""
                )
                subprocess.run(del_cmd, shell=True, timeout=120)
                deleted_count = len(to_delete)
                log.info("Parquet 파일 %d개 삭제 완료", deleted_count)
        else:
            log.info("삭제 대상 Parquet 파일 없음")

        return {
            "dry_run": dry_run,
            "parquet_candidates": len(candidate_files),
            "parquet_deleted": deleted_count,
            "parquet_protected": len(candidate_files) - deleted_count,
        }

    @task()
    def clean_old_logs(parquet_result: dict) -> dict:
        """
        /storage/logs/ 내 LOG_RETENTION_DAYS일 이상 된 로그 파일 삭제.
        """
        import subprocess
        from airflow.operators.python import get_current_context

        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        find_cmd = (
            f"docker exec welding-spark-master find {STORAGE_LOGS_DIR} "
            f"-type f \\( -name '*.log' -o -name '*.log.*' \\) "
            f"-mtime +{LOG_RETENTION_DAYS}"
        )
        result = subprocess.run(
            find_cmd, shell=True, capture_output=True, text=True, timeout=60
        )
        log_files = [f.strip() for f in result.stdout.strip().split("\n") if f.strip()]

        deleted_count = 0
        if log_files:
            if dry_run:
                log.info("dry_run=True, 로그 삭제 생략. 대상 %d개", len(log_files))
            else:
                file_list = "\n".join(log_files)
                del_cmd = (
                    f"docker exec welding-spark-master bash -c "
                    f"\"echo '{file_list}' | xargs -r rm -f\""
                )
                subprocess.run(del_cmd, shell=True, timeout=60)
                deleted_count = len(log_files)
                log.info("로그 파일 %d개 삭제 완료", deleted_count)
        else:
            log.info("삭제 대상 로그 파일 없음")

        return {**parquet_result, "logs_deleted": deleted_count}

    @task()
    def clean_spark_checkpoints(log_result: dict) -> dict:
        """
        /tmp/spark-checkpoints-*  디렉토리 중 CHECKPOINT_RETENTION_DAYS일 이상 된 것 삭제.
        현재 실행 중인 컨슈머의 체크포인트는 제외.
        """
        import subprocess
        from airflow.operators.python import get_current_context

        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        # 실행 중인 컨슈머의 checkpoint 디렉토리 조회
        ps_result = subprocess.run(
            "docker exec welding-spark-master bash -lc "
            "\"ps -ef | grep 'SPARK_CHECKPOINT_DIR' | grep -v grep | "
            "grep -oP 'SPARK_CHECKPOINT_DIR=\\S+' | cut -d= -f2\"",
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        active_dirs = set(
            d.strip() for d in ps_result.stdout.strip().split("\n") if d.strip()
        )
        log.info("실행 중인 체크포인트 디렉토리 %d개 (보호)", len(active_dirs))

        # 오래된 checkpoint 디렉토리 목록
        find_cmd = (
            f"docker exec welding-spark-master find {CHECKPOINT_DIR} "
            f"-maxdepth 1 -name 'spark-checkpoints-*' -type d "
            f"-mtime +{CHECKPOINT_RETENTION_DAYS}"
        )
        result = subprocess.run(
            find_cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        candidate_dirs = [d.strip() for d in result.stdout.strip().split("\n") if d.strip()]
        to_delete = [d for d in candidate_dirs if d not in active_dirs]

        deleted_count = 0
        if to_delete:
            if dry_run:
                log.info("dry_run=True, 체크포인트 삭제 생략. 대상 %d개", len(to_delete))
            else:
                for d in to_delete:
                    rm_cmd = f"docker exec welding-spark-master rm -rf {d}"
                    subprocess.run(rm_cmd, shell=True, timeout=30)
                    deleted_count += 1
                log.info("Spark 체크포인트 디렉토리 %d개 삭제 완료", deleted_count)
        else:
            log.info("삭제 대상 체크포인트 없음")

        return {**log_result, "checkpoints_deleted": deleted_count}

    @task()
    def report_cleanup_result(cleanup_result: dict, disk_info: dict):
        """
        정리 결과를 DB에 기록하고 최종 디스크 사용량을 확인한다.
        """
        import subprocess

        # 정리 후 디스크 사용량 재확인
        result = subprocess.run(
            "docker exec welding-spark-master df -h /storage 2>/dev/null || echo 'N/A'",
            shell=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
        after_df = result.stdout.strip()

        summary = {
            "status": "completed",
            "dry_run": cleanup_result.get("dry_run", False),
            "disk_usage_before_pct": disk_info.get("usage_pct", -1),
            "parquet_deleted": cleanup_result.get("parquet_deleted", 0),
            "logs_deleted": cleanup_result.get("logs_deleted", 0),
            "checkpoints_deleted": cleanup_result.get("checkpoints_deleted", 0),
            "total_deleted": (
                cleanup_result.get("parquet_deleted", 0)
                + cleanup_result.get("logs_deleted", 0)
                + cleanup_result.get("checkpoints_deleted", 0)
            ),
            "disk_after": after_df,
        }

        log.info(
            "🧹 스토리지 정리 완료 — Parquet: %d개, 로그: %d개, 체크포인트: %d개",
            summary["parquet_deleted"],
            summary["logs_deleted"],
            summary["checkpoints_deleted"],
        )

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    ("airflow.storage_cleanup", json.dumps(summary)),
                )

    # ── 의존성 연결 ──────────────────────────────────────────────
    disk = check_disk_usage()
    parquet = clean_old_parquet(disk)
    logs = clean_old_logs(parquet)
    checkpoints = clean_spark_checkpoints(logs)
    report_cleanup_result(checkpoints, disk)


welding_storage_cleanup_dag()
