"""
welding_streaming_monitor.py - Airflow 3 Version
===============================================
Modern TaskFlow API with Branching for Airflow 3.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task
from airflow.providers.standard.operators.bash import BashOperator

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
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()
HEALTH_WINDOW_MIN = 15
REASSEMBLY_HARD_FAIL_THRESHOLD = int(os.getenv("REASSEMBLY_HARD_FAIL_THRESHOLD", "0"))
TOPIC_RAW_LASER_A = os.getenv("TOPIC_RAW_LASER_A", "welding.raw.laser_a.v1")
TOPIC_RAW_LASER_B = os.getenv("TOPIC_RAW_LASER_B", "welding.raw.laser_b.v1")
SOURCE_LASER_A = f"kafka://{TOPIC_RAW_LASER_A}"
SOURCE_LASER_B = f"kafka://{TOPIC_RAW_LASER_B}"
SPARK_SUBMIT_CMD = (
    "docker exec welding-spark-master bash -lc '"
    "LINE_COUNT=${LINE_COUNT:-3}; "
    "CONSUMER_COUNT=${CONSUMER_COUNT:-$((LINE_COUNT * 2))}; "
    "SPARK_STREAMING_CORES_MAX=${SPARK_STREAMING_CORES_MAX:-1}; "
    "SPARK_STREAMING_EXECUTOR_CORES=${SPARK_STREAMING_EXECUTOR_CORES:-1}; "
    "SPARK_STREAMING_EXECUTOR_MEMORY=${SPARK_STREAMING_EXECUTOR_MEMORY:-1g}; "
    "if [ $((CONSUMER_COUNT % 2)) -ne 0 ] || [ \"${CONSUMER_COUNT}\" -lt 2 ]; then "
    "  echo \"CONSUMER_COUNT must be even and >=2\"; exit 1; "
    "fi; "
    "pids=$(pgrep -f \"spark_streaming.py\" 2>/dev/null | grep -vw \"$$\" || true); "
    "if [ -n \"$pids\" ]; then kill -TERM $pids || true; sleep 5; fi; "
    "pids2=$(pgrep -f \"spark_streaming.py\" 2>/dev/null | grep -vw \"$$\" || true); "
    "if [ -n \"$pids2\" ]; then kill -KILL $pids2 || true; fi; "
    "mkdir -p /tmp/.ivy2; "
    "for consumer_id in $(seq 1 ${CONSUMER_COUNT}); do "
    "  if [ $((consumer_id % 2)) -eq 1 ]; then "
    "    topic=\"welding.raw.laser_a.v1\"; channel=\"laser_a\"; group_id=\"welding-stream-laser-a\"; "
    "  else "
    "    topic=\"welding.raw.laser_b.v1\"; channel=\"laser_b\"; group_id=\"welding-stream-laser-b\"; "
    "  fi; "
    "  rm -rf /tmp/spark-checkpoints-consumer-${consumer_id}; "
    "  nohup env TOPIC_RAW=\"${topic}\" CHANNEL_FILTER=\"${channel}\" KAFKA_GROUP_ID=\"${group_id}\" SPARK_CHECKPOINT_DIR=\"/tmp/spark-checkpoints-consumer-${consumer_id}\" "
    "    /opt/spark/bin/spark-submit --master spark://spark-master:7077 "
    "    --conf spark.cores.max=${SPARK_STREAMING_CORES_MAX} "
    "    --conf spark.executor.cores=${SPARK_STREAMING_EXECUTOR_CORES} "
    "    --conf spark.executor.memory=${SPARK_STREAMING_EXECUTOR_MEMORY} "
    "    --conf spark.jars.ivy=/tmp/.ivy2 "
    "    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3 "
    "    /opt/spark/apps/spark_streaming.py >/tmp/spark_streaming_consumer_${consumer_id}.log 2>&1 & "
    "done;'"
)


def _recent_channel_counts(window_minutes: int) -> tuple[int, int]:
    """최근 window_minutes분 내 channel별 summary 행 수를 반환한다.

    [fix] source_file URI 고정 필터 제거:
    실제 DB의 pattern_summary.source_file에는 'kafka://...benchmark...' 형태의 값이
    저장되어 있어 'kafka://welding.raw.laser_a.v1' 고정 문자열과 불일치 → 항상 0 반환.
    channel 컬럼(1=laser_a, 0=laser_b) + processed_at 시간 윈도우 기반으로 교체.
    """
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT channel, COUNT(*)
                FROM welding.pattern_summary
                WHERE processed_at >= NOW() - (%s || ' minutes')::interval
                GROUP BY channel
                """,
                (window_minutes,),
            )
            rows = cur.fetchall()

    counts = {0: 0, 1: 0}  # 0=laser_b, 1=laser_a
    for channel, count in rows:
        counts[int(channel)] = int(count)
    return counts[1], counts[0]  # (laser_a_count, laser_b_count)


def _recent_reassembly_issues(window_minutes: int) -> tuple[int, int]:
    """Return latest-key incomplete/hard-fail counts from reassembly_audit."""
    try:
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    )
                    """
                )
                cur.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (
                            product_instance_id, line_id, lead_num, channel, replay_iteration
                        )
                            reassembly_status
                        FROM welding.reassembly_audit
                        WHERE observed_at >= NOW() - (%s || ' minutes')::interval
                        ORDER BY
                            product_instance_id,
                            line_id,
                            lead_num,
                            channel,
                            replay_iteration,
                            observed_at DESC
                    )
                    SELECT
                        COUNT(*) FILTER (WHERE reassembly_status = 'incomplete_chunks') AS incomplete_count,
                        COUNT(*) FILTER (
                            WHERE reassembly_status IN ('mixed_chunk_metadata', 'sample_count_mismatch')
                        ) AS hard_fail_count
                    FROM latest
                    """,
                    (window_minutes,),
                )
                row = cur.fetchone()
                if not row:
                    return 0, 0
                return int(row[0] or 0), int(row[1] or 0)
    except Exception as exc:
        log.warning("reassembly_audit query skipped: %s", exc)
        return 0, 0



def _send_external_alert(title: str, details: dict) -> None:
    """Send optional external alert to webhook endpoint."""
    if not ALERT_WEBHOOK_URL:
        return
    payload = {
        "text": title,
        "details": details,
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        ALERT_WEBHOOK_URL,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception as exc:
        log.warning("External alert webhook failed: %s", exc)

@dag(
    dag_id="welding_streaming_monitor",
    schedule="*/10 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["welding", "airflow3", "streaming"],
    default_args={"owner": "welding-team", "retries": 1}
)
def welding_streaming_monitor_dag():

    @task.branch()
    def check_streaming_health():
        try:
            a_count, b_count = _recent_channel_counts(HEALTH_WINDOW_MIN)
            incomplete_count, hard_fail_count = _recent_reassembly_issues(HEALTH_WINDOW_MIN)
            log.info(
                "Recent channel records (%s min): laser_a=%s laser_b=%s / reassembly: incomplete=%s hard_fail=%s",
                HEALTH_WINDOW_MIN,
                a_count,
                b_count,
                incomplete_count,
                hard_fail_count,
            )
            is_healthy = (
                a_count > 0
                and b_count > 0
                and hard_fail_count <= REASSEMBLY_HARD_FAIL_THRESHOLD
            )
            return "log_healthy_heartbeat" if is_healthy else "restart_spark_streaming"
        except Exception as e:
            log.error(f"Health check failed: {e}")
            return "restart_spark_streaming"

    @task()
    def log_healthy_heartbeat():
        a_count, b_count = _recent_channel_counts(HEALTH_WINDOW_MIN)
        incomplete_count, hard_fail_count = _recent_reassembly_issues(HEALTH_WINDOW_MIN)
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.streaming_monitor",
                        (
                            "{\"status\":\"healthy\","
                            f"\"window_min\":{HEALTH_WINDOW_MIN},"
                            f"\"laser_a_count\":{a_count},"
                            f"\"laser_b_count\":{b_count},"
                            f"\"reassembly_incomplete_count\":{incomplete_count},"
                            f"\"reassembly_hard_fail_count\":{hard_fail_count}"
                            "}"
                        ),
                    ),
                )

    restart_spark = BashOperator(
        task_id="restart_spark_streaming",
        bash_command=SPARK_SUBMIT_CMD
    )

    @task()
    def verify_restart():
        import time

        time.sleep(60)
        a_count, b_count = _recent_channel_counts(2)
        incomplete_count, hard_fail_count = _recent_reassembly_issues(2)
        log.info(
            "Post-restart records (2 min): laser_a=%s laser_b=%s / reassembly: incomplete=%s hard_fail=%s",
            a_count, b_count, incomplete_count, hard_fail_count,
        )
        if not (
            a_count > 0
            and b_count > 0
            and hard_fail_count <= REASSEMBLY_HARD_FAIL_THRESHOLD
        ):
            raise RuntimeError(
                "restart verify failed: "
                f"laser_a={a_count}, laser_b={b_count}, "
                f"reassembly_hard_fail={hard_fail_count}"
            )

    @task(trigger_rule="one_failed")
    def notify_failure():
        details = {"status": "restart_failed"}
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    ("airflow.streaming_monitor", "{\"status\":\"restart_failed\"}"),
                )
        _send_external_alert("welding_streaming_monitor restart failed", details)

    # Dependency Flow
    branch = check_streaming_health()
    branch >> log_healthy_heartbeat()
    verify = verify_restart()
    branch >> restart_spark >> verify >> notify_failure()

welding_streaming_monitor_dag()


