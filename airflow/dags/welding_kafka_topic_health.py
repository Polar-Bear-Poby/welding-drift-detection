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
                lines = result.stdout.strip().split("\n")
                total_lag = 0
                max_lag = 0
                partition_count = 0

                for line in lines:
                    parts = line.split()
                    # 출력 컬럼: GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
                    if len(parts) >= 6 and parts[5].lstrip("-").isdigit():
                        lag_val = int(parts[5]) if parts[5] != "-" else 0
                        total_lag += lag_val
                        max_lag = max(max_lag, lag_val)
                        partition_count += 1

                lag_report[group] = {
                    "total_lag": total_lag,
                    "max_partition_lag": max_lag,
                    "partition_count": partition_count,
                    "status": "high_lag" if max_lag > LAG_ALERT_THRESHOLD else "normal",
                }
                log.info(
                    "그룹 %s — total_lag: %d, max_partition_lag: %d",
                    group, total_lag, max_lag,
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
        high_lag_groups = [
            g for g, d in lag_report.items()
            if d.get("status") == "high_lag"
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
                                "high_lag_groups": high_lag_groups,
                                "lag_threshold": LAG_ALERT_THRESHOLD,
                            }
                        ),
                    ),
                )

        if high_lag_groups:
            log.warning("🚨 High Lag 감지: %s", high_lag_groups)
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
        high_lag_groups = {
            g: d for g, d in lag_report.items()
            if d.get("status") == "high_lag"
        }
        log.error(
            "❌ Consumer Lag 임계값 초과! 그룹별 상세:\n%s",
            json.dumps(high_lag_groups, indent=2),
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
                                "high_lag_groups": high_lag_groups,
                                "threshold": LAG_ALERT_THRESHOLD,
                                "action": "Check producer/consumer throughput balance",
                            }
                        ),
                    ),
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
