"""
welding_kafka_topic_health.py - Airflow 3 Version
==================================================
1시간 주기로 Kafka 토픽 존재 여부와 Consumer Lag을 확인한다.

모니터링 대상 토픽:
  - welding.raw.laser_a.v1  (laser_a 스트리밍)
  - welding.raw.laser_b.v1  (laser_b 스트리밍)
  - welding.raw.v1           (기본 raw 토픽)

Consumer Lag 모니터링 대상 그룹:
  - welding-stream-laser-a
  - welding-stream-laser-b

DAG 흐름:
    check_topics_exist
        ├── [토픽 정상] → check_consumer_lag → analyze_lag_result
        │                     ├── [Lag 정상] → log_kafka_healthy
        │                     └── [Lag 초과] → alert_high_lag
        └── [토픽 없음] → recreate_missing_topics → alert_topic_recreated
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
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
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()

KAFKA_BOOTSTRAP = "kafka:9092"
KAFKA_CONTAINER = "welding-kafka"

# 모니터링 대상 토픽
REQUIRED_TOPICS = [
    "welding.raw.v1",
    "welding.raw.laser_a.v1",
    "welding.raw.laser_b.v1",
]

# 모니터링 대상 컨슈머 그룹
CONSUMER_GROUPS = [
    "welding-stream-laser-a",
    "welding-stream-laser-b",
]

# Consumer Lag 경보 임계값 (파티션당 누적 미처리 메시지 수)
LAG_ALERT_THRESHOLD = 10000


def _send_external_alert(title: str, details: dict) -> None:
    """Send optional external alert to webhook endpoint."""
    if not ALERT_WEBHOOK_URL:
        return
    body = json.dumps({"text": title, "details": details}).encode("utf-8")
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
    dag_id="welding_kafka_topic_health",
    schedule="0 * * * *",  # 매시간 정각
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "kafka", "monitoring"],
    default_args={
        "owner": "welding-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    doc_md="""
## welding_kafka_topic_health

**목적**: 매시간 Kafka 토픽 존재 여부와 Consumer Lag을 확인한다.

**핸드오버 이슈 해결**: `UnknownTopicOrPartitionException`으로 컨슈머가 종료되는
문제를 사전에 탐지하고 토픽을 자동으로 재생성한다.

**모니터링 토픽**: welding.raw.v1, welding.raw.laser_a.v1, welding.raw.laser_b.v1

**Lag 경보 임계값**: 파티션당 {threshold:,} 메시지
    """.format(threshold=LAG_ALERT_THRESHOLD),
)
def welding_kafka_topic_health_dag():

    @task.branch()
    def check_topics_exist():
        """
        welding-kafka 컨테이너에서 필수 토픽 존재 여부를 확인한다.
        하나라도 없으면 재생성 브랜치로 분기.
        """
        try:
            result = subprocess.run(
                f"docker exec {KAFKA_CONTAINER} "
                f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} --list",
                shell=True,
                capture_output=True,
                text=True,
                timeout=30,
            )
            existing_topics = set(result.stdout.strip().split("\n"))
            missing = [t for t in REQUIRED_TOPICS if t not in existing_topics]

            log.info("Kafka 토픽 현황 — 존재: %d개, 누락: %s", len(existing_topics), missing)

            # heartbeat 기록
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                        "VALUES (%s, %s::jsonb)",
                        (
                            "airflow.kafka_topic_health.topic_check",
                            json.dumps(
                                {
                                    "required_topics": REQUIRED_TOPICS,
                                    "missing_topics": missing,
                                    "status": "missing" if missing else "ok",
                                }
                            ),
                        ),
                    )

            if missing:
                log.error("필수 토픽 누락: %s", missing)
                return "recreate_missing_topics"
            return "check_consumer_lag"

        except Exception as exc:
            log.error("토픽 확인 실패: %s", exc)
            return "recreate_missing_topics"

    recreate_missing_topics = BashOperator(
        task_id="recreate_missing_topics",
        bash_command=(
            f"docker exec {KAFKA_CONTAINER} bash -c \""
            f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} "
            f"--create --if-not-exists --topic welding.raw.v1 "
            f"--partitions 8 --replication-factor 1 "
            f"--config retention.ms=604800000 --config max.message.bytes=5242880 && "
            f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} "
            f"--create --if-not-exists --topic welding.raw.laser_a.v1 "
            f"--partitions 8 --replication-factor 1 "
            f"--config retention.ms=604800000 --config max.message.bytes=5242880 && "
            f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP} "
            f"--create --if-not-exists --topic welding.raw.laser_b.v1 "
            f"--partitions 8 --replication-factor 1 "
            f"--config retention.ms=604800000 --config max.message.bytes=5242880\""
        ),
        execution_timeout=timedelta(minutes=2),
    )

    @task()
    def alert_topic_recreated():
        """토픽 재생성 후 경보를 DB에 기록한다."""
        log.warning("⚠️ Kafka 토픽이 누락되어 재생성되었습니다. 컨슈머 재시작을 권장합니다.")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.kafka_topic_health",
                        json.dumps(
                            {
                                "status": "topics_recreated",
                                "action": "consumer_restart_recommended",
                                "topics": REQUIRED_TOPICS,
                            }
                        ),
                    ),
                )
        _send_external_alert(
            "welding_kafka_topic_health recreated missing topics",
            {"status": "topics_recreated", "topics": REQUIRED_TOPICS},
        )

    @task()
    def check_consumer_lag() -> dict:
        """
        kafka-consumer-groups 명령으로 각 그룹의 Consumer Lag을 측정한다.
        그룹별 총 lag과 파티션별 최대 lag을 반환한다.
        """
        lag_report = {}

        for group in CONSUMER_GROUPS:
            try:
                result = subprocess.run(
                    f"docker exec {KAFKA_CONTAINER} "
                    f"kafka-consumer-groups --bootstrap-server {KAFKA_BOOTSTRAP} "
                    f"--group {group} --describe",
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=30,
                )
                if result.returncode != 0:
                    raise RuntimeError(result.stderr.strip() or "kafka-consumer-groups failed")
                lines = [
                    line.strip()
                    for line in result.stdout.strip().split("\n")
                    if line.strip() and not line.startswith("GROUP")
                ]
                total_lag = 0
                max_lag = 0
                partition_count = 0
                unassigned_partitions = 0
                parse_errors = 0

                for line in lines:
                    parts = line.split()
                    # Expected columns:
                    # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
                    if len(parts) < 6:
                        parse_errors += 1
                        continue
                    lag_token = parts[5]
                    consumer_id = parts[6] if len(parts) >= 7 else "-"
                    if lag_token == "-":
                        unassigned_partitions += 1
                        partition_count += 1
                        continue
                    if not lag_token.lstrip("-").isdigit():
                        parse_errors += 1
                        continue
                    lag_val = int(lag_token)
                    if lag_val < 0:
                        parse_errors += 1
                        continue
                    total_lag += lag_val
                    max_lag = max(max_lag, lag_val)
                    partition_count += 1
                    if consumer_id == "-":
                        unassigned_partitions += 1

                if parse_errors > 0:
                    status = "parse_error"
                elif unassigned_partitions > 0:
                    status = "unassigned"
                elif max_lag > LAG_ALERT_THRESHOLD:
                    status = "high_lag"
                else:
                    status = "normal"

                lag_report[group] = {
                    "total_lag": total_lag,
                    "max_partition_lag": max_lag,
                    "partition_count": partition_count,
                    "unassigned_partitions": unassigned_partitions,
                    "parse_errors": parse_errors,
                    "status": status,
                }
                log.info(
                    "그룹 %s — total_lag: %d, max_partition_lag: %d, unassigned: %d, parse_errors: %d",
                    group, total_lag, max_lag, unassigned_partitions, parse_errors,
                )

            except Exception as exc:
                log.error("그룹 %s lag 확인 실패: %s", group, exc)
                lag_report[group] = {"error": str(exc), "status": "unknown"}

        return lag_report

    @task.branch()
    def analyze_lag_result(lag_report: dict) -> str:
        """
        lag_report를 분석하여 경보 여부를 판단한다.
        하나라도 high_lag이면 alert 브랜치로.
        """
        problematic_groups = [
            g for g, d in lag_report.items()
            if d.get("status") in {"high_lag", "unassigned", "parse_error", "unknown"}
        ]

        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.kafka_topic_health.lag_check",
                        json.dumps(
                            {
                                "lag_report": lag_report,
                                "problematic_groups": problematic_groups,
                                "lag_threshold": LAG_ALERT_THRESHOLD,
                            }
                        ),
                    ),
                )

        if problematic_groups:
            log.warning("🚨 Kafka consumer 상태 이상 감지: %s", problematic_groups)
            return "alert_high_lag"
        return "log_kafka_healthy"

    @task()
    def log_kafka_healthy(lag_report: dict):
        """Kafka 및 Consumer Lag 정상 상태 heartbeat 기록."""
        log.info("✅ Kafka 토픽 및 Consumer Lag 정상")
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.kafka_topic_health",
                        json.dumps({"status": "healthy", "lag_report": lag_report}),
                    ),
                )

    @task()
    def alert_high_lag(lag_report: dict):
        """Consumer Lag 경보 — DB에 상세 정보 기록."""
        problem_groups = {
            g: d for g, d in lag_report.items()
            if d.get("status") in {"high_lag", "unassigned", "parse_error", "unknown"}
        }
        log.error(
            "❌ Consumer 상태 이상 감지! 그룹별 상세:\n%s",
            json.dumps(problem_groups, indent=2),
        )
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO welding.pipeline_heartbeat (component_name, details) "
                    "VALUES (%s, %s::jsonb)",
                    (
                        "airflow.kafka_topic_health",
                        json.dumps(
                            {
                                "status": "high_lag_alert",
                                "problem_groups": problem_groups,
                                "threshold": LAG_ALERT_THRESHOLD,
                                "action": "Check producer/consumer throughput balance",
                            }
                        ),
                    ),
                )
        _send_external_alert(
            "welding_kafka_topic_health consumer issue detected",
            {"status": "high_lag_alert", "problem_groups": problem_groups},
        )

    # ── 의존성 연결 ──────────────────────────────────────────────
    topic_branch = check_topics_exist()

    # 토픽 누락 경로
    topic_branch >> recreate_missing_topics >> alert_topic_recreated()

    # 토픽 정상 경로 → Lag 체크
    lag_data = check_consumer_lag()
    lag_branch = analyze_lag_result(lag_data)
    topic_branch >> lag_data >> lag_branch

    lag_branch >> log_kafka_healthy(lag_data)
    lag_branch >> alert_high_lag(lag_data)


welding_kafka_topic_health_dag()
