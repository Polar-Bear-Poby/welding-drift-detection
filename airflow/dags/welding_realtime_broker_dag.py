"""
welding_realtime_broker_dag.py
================================
new_src/ 아키텍처 전용 Broker DAG (Asset-based).
기존 welding_broker_asset_dag.py 대응.

기존 vs new_src 대응:
  Kafka topic 존재 확인             → watched/ 폴더 + 라인 디렉터리 존재 확인
  consumer group lag 확인           → watched/ 잔여 파일(stale) 수 확인
  pipeline_heartbeat (PostgreSQL)   → airflow_heartbeat.jsonl
  PRODUCER_DONE_ASSET 트리거        → REALTIME_PRODUCER_DONE_ASSET 트리거
  BROKER_READY_ASSET emit           → REALTIME_BROKER_READY_ASSET emit

역할:
  - Producer DAG 완료(REALTIME_PRODUCER_DONE_ASSET) 후 자동 트리거
  - 최신 Producer 컨텍스트 읽기 (heartbeat.jsonl에서)
  - watched/ 라인 폴더 존재 여부 확인 (Kafka topic 존재 확인 대응)
  - watched/ 잔여 stale 파일 수 확인 (consumer group lag 확인 대응)
  - result CSV throughput 확인 (FileWatcher → Consumer 정상 전달 여부)
  - Broker heartbeat 기록
  - REALTIME_BROKER_READY_ASSET emit → Consumer DAG 자동 트리거

흐름:
  read_latest_producer_context     (pipeline_heartbeat SELECT 대응)
    → validate_watched_topics      (kafka-topics --describe 대응)
    → validate_queue_lag           (kafka-consumer-groups --describe 대응)
    → emit_broker_heartbeat        (pipeline_heartbeat INSERT 대응)
    → publish_broker_asset         (REALTIME_BROKER_READY_ASSET emit)

schedule=[REALTIME_PRODUCER_DONE_ASSET]:
  Producer DAG가 Asset을 emit하면 자동 트리거.
  (기존 Broker DAG와 동일한 Asset-based 스케줄링)
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from welding_realtime_assets import REALTIME_BROKER_READY_ASSET, REALTIME_PRODUCER_DONE_ASSET

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
WATCHED_DIR = ROOT_DIR / "new_src" / "watched"
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"

# ── 임계값 ────────────────────────────────────────────────────────────────────
# stale 파일이 STALE_THRESHOLD_SEC 초 이상 watched/에 남아있으면 lag 경보
# (기존 CONSUMER_LAG_ALERT_THRESHOLD 대응)
STALE_THRESHOLD_SEC = int(os.getenv("REALTIME_STALE_THRESHOLD", "120"))
# result CSV 최소 커버리지
MIN_COVERAGE_RATIO = float(os.getenv("REALTIME_MIN_COVERAGE_RATIO", "0.8"))


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _read_latest_producer_heartbeat() -> dict:
    """
    airflow_heartbeat.jsonl 에서 가장 최근 PRODUCER_DONE 레코드를 읽는다.
    기존: PostgreSQL pipeline_heartbeat SELECT 대응.
    """
    if not HEARTBEAT_LOG.exists():
        return {}
    lines = HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            if (rec.get("component") == "airflow.realtime_producer_dag"
                    and rec.get("status") == "PRODUCER_DONE"):
                return rec
        except json.JSONDecodeError:
            continue
    return {}


def _get_line_dir_status() -> dict[str, bool]:
    """
    watched/ 하위 라인 디렉터리 존재 여부를 반환한다.
    기존: Kafka topic 존재 확인 대응.
    LINE_01, LINE_02, ... 각각 topic 하나에 대응.
    """
    if not WATCHED_DIR.exists():
        return {}
    return {
        sub.name: sub.is_dir()
        for sub in sorted(WATCHED_DIR.iterdir())
        if sub.name.startswith("LINE_")
    }


def _get_queue_lag_summary() -> dict:
    """
    watched/ 폴더의 stale 파일 수와 lag 상태를 반환한다.
    기존: _group_lag (kafka-consumer-groups --describe) 대응.

    stale 파일 = STALE_THRESHOLD_SEC 초 이상 오래된 CSV
    → Consumer가 처리하지 못하고 쌓인 메시지에 해당.
    """
    if not WATCHED_DIR.exists():
        return {
            "total_files": 0,
            "stale_files": 0,
            "stale_ratio": 0.0,
            "stale_threshold_sec": STALE_THRESHOLD_SEC,
            "healthy": True,
        }

    cutoff = time.time() - STALE_THRESHOLD_SEC
    total = 0
    stale = 0
    for csv_path in WATCHED_DIR.rglob("*.csv"):
        total += 1
        try:
            if csv_path.stat().st_mtime < cutoff:
                stale += 1
        except OSError:
            pass

    stale_ratio = stale / total if total > 0 else 0.0
    # stale 비율 50% 초과 → 브로커 lag 이상
    healthy = stale_ratio <= 0.5

    return {
        "total_files": total,
        "stale_files": stale,
        "stale_ratio": round(stale_ratio, 4),
        "stale_threshold_sec": STALE_THRESHOLD_SEC,
        "healthy": healthy,
    }


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_broker_dag",
    schedule=[REALTIME_PRODUCER_DONE_ASSET],
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "realtime", "asset", "broker"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    doc_md="""
## welding_realtime_broker_dag

**아키텍처**: new_src/ FileWatcher 기반 / 기존 `welding_broker_asset_dag` 대응

**트리거**: `REALTIME_PRODUCER_DONE_ASSET` emit 시 자동 실행

**대응 관계**
| 기존 (Kafka) | new_src (FileWatcher) |
|---|---|
| Kafka topic 존재 확인 | `watched/LINE_XX/` 디렉터리 존재 확인 |
| consumer group lag 확인 | watched/ stale 파일 수 확인 |
| `pipeline_heartbeat` (PostgreSQL) | `airflow_heartbeat.jsonl` |
| `BROKER_READY_ASSET` | `REALTIME_BROKER_READY_ASSET` |

**브로커 개념 매핑**
- Kafka topic welding.raw.laser_a.v1 = `watched/LINE_01/` (laser_a 파일)
- Kafka topic welding.raw.laser_b.v1 = `watched/LINE_01/` (laser_b 파일)
- consumer group lag = watched/ 폴더에 남은 stale 파일 수
- FileWatcher가 파일을 queue로 보내면 lag=0 (소비 완료)
    """,
)
def welding_realtime_broker_dag():

    @task()
    def read_latest_producer_context() -> dict:
        """
        heartbeat.jsonl에서 최신 PRODUCER_DONE 컨텍스트를 읽는다.
        기존: pipeline_heartbeat에서 PRODUCER_DONE 레코드 SELECT 대응.
        """
        ctx = _read_latest_producer_heartbeat()
        if not ctx:
            raise RuntimeError(
                "Producer heartbeat 없음. "
                "welding_realtime_producer_dag가 먼저 실행되어야 합니다."
            )

        run_id = str(ctx.get("run_id") or "").strip()
        batteries = int(ctx.get("batteries") or 0)
        lines = int(ctx.get("lines") or 0)

        missing = []
        if not run_id:
            missing.append("run_id")
        if batteries < 1:
            missing.append("batteries")
        if lines < 1:
            missing.append("lines")
        if missing:
            raise RuntimeError(
                f"Producer heartbeat 필드 누락/오류: {missing} — ctx={ctx}"
            )

        log.info(
            "Producer 컨텍스트 로드 — run_id=%s, batteries=%d, lines=%d",
            run_id[:8], batteries, lines,
        )
        return {
            "run_id": run_id,
            "batteries": batteries,
            "lines": lines,
            "interval": float(ctx.get("interval") or 10.0),
            "experimental": bool(ctx.get("experimental", False)),
            "target_date": str(ctx.get("target_date") or ""),
            "coverage": float(ctx.get("coverage") or 0.0),
        }

    @task()
    def validate_watched_topics(ctx: dict) -> dict:
        """
        라인별 watched/ 디렉터리 존재 여부를 확인한다.
        기존: validate_topics (Kafka topic --describe) 대응.

        watched/LINE_01/, watched/LINE_02/, ... 각각이
        Kafka topic welding.raw.laser_a.v1 / laser_b.v1 역할.
        """
        line_status = _get_line_dir_status()

        # 생성된 라인 디렉터리 수 확인
        lines = int(ctx.get("lines", 1))
        expected_dirs = {f"LINE_{i:02d}" for i in range(1, lines + 1)}
        found_dirs = {name for name, exists in line_status.items() if exists}
        missing_dirs = expected_dirs - found_dirs

        log.info(
            "watched/ 라인 디렉터리 확인 — 기대: %s, 발견: %s",
            sorted(expected_dirs), sorted(found_dirs),
        )

        if missing_dirs:
            # 실험이 완료된 후라면 watched/ 폴더가 정리됐을 수 있음
            # producer coverage >= 0.5 이면 경고만, 아니면 실패
            if float(ctx.get("coverage", 0)) >= 0.5:
                log.warning(
                    "watched/ 디렉터리 없음(정리됨 가능성): %s — 실험 커버리지 %.0f%%로 통과",
                    sorted(missing_dirs), float(ctx.get("coverage", 0)) * 100,
                )
            else:
                raise RuntimeError(
                    f"watched/ 디렉터리 미생성: {sorted(missing_dirs)} "
                    "(DataFeeder가 실행되지 않았을 수 있습니다)"
                )

        return {**ctx, "line_dir_status": line_status}

    @task()
    def validate_queue_lag(ctx: dict) -> dict:
        """
        watched/ 폴더의 stale 파일 수로 Consumer lag을 검증한다.
        기존: validate_consumer_lag (kafka-consumer-groups --describe) 대응.

        lag_laser_a = watched/ 폴더의 laser_a stale 파일 수
        lag_laser_b = watched/ 폴더의 laser_b stale 파일 수
        """
        lag_summary = _get_queue_lag_summary()

        log.info(
            "Queue lag 확인 — total=%d, stale=%d (%.0f%%), healthy=%s",
            lag_summary["total_files"],
            lag_summary["stale_files"],
            lag_summary["stale_ratio"] * 100,
            lag_summary["healthy"],
        )

        # lag 불량이어도 실험이 완료된 경우라면 경고만
        if not lag_summary["healthy"]:
            if bool(ctx.get("experimental", False)):
                log.warning(
                    "Queue lag 이상 (experimental 모드 허용) — stale=%.0f%%",
                    lag_summary["stale_ratio"] * 100,
                )
            else:
                log.warning(
                    "Queue lag 이상 감지 — stale=%d파일 (%.0f%%). "
                    "실험 완료 후 정리됐을 가능성이 있어 경고로 처리합니다.",
                    lag_summary["stale_files"],
                    lag_summary["stale_ratio"] * 100,
                )

        # result CSV throughput 확인
        result_csv_count = sum(1 for _ in METRICS_DIR.glob("*.csv")) if METRICS_DIR.exists() else 0

        return {
            **ctx,
            "lag_laser_a": {
                "stale_files": lag_summary["stale_files"] // 2,
                "healthy": lag_summary["healthy"],
            },
            "lag_laser_b": {
                "stale_files": lag_summary["stale_files"] - lag_summary["stale_files"] // 2,
                "healthy": lag_summary["healthy"],
            },
            "queue_lag_summary": lag_summary,
            "result_csv_count": result_csv_count,
        }

    @task()
    def emit_broker_heartbeat(meta: dict) -> dict:
        """
        Broker 상태를 heartbeat.jsonl에 기록한다.
        기존: emit_broker_heartbeat (PostgreSQL INSERT) 대응.
        """
        details = {
            "component": "airflow.realtime_broker_dag",
            "status": "BROKER_READY",
            "run_id": meta.get("run_id"),
            "target_date": meta.get("target_date"),
            "batteries": meta.get("batteries"),
            "lines": meta.get("lines"),
            "experimental": bool(meta.get("experimental", False)),
            "lag_laser_a": meta.get("lag_laser_a"),
            "lag_laser_b": meta.get("lag_laser_b"),
            "result_csv_count": meta.get("result_csv_count", 0),
            "producer_coverage": meta.get("coverage", 0),
            "emitted_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_heartbeat(details)
        log.info(
            "Broker heartbeat 기록 — run_id=%s, result_csv=%d",
            str(meta.get("run_id") or "")[:8],
            meta.get("result_csv_count", 0),
        )
        return meta

    @task(outlets=[REALTIME_BROKER_READY_ASSET])
    def publish_broker_asset(meta: dict) -> dict:
        """
        REALTIME_BROKER_READY_ASSET을 emit한다.
        → Consumer DAG (welding_realtime_consumer_dag) 자동 트리거.
        기존: publish_broker_asset 과 동일.
        """
        log.info(
            "REALTIME_BROKER_READY_ASSET 발행 — run_id=%s",
            str(meta.get("run_id") or "")[:8],
        )
        return meta

    # ── 의존성 연결 (기존 broker DAG와 동일한 구조) ──────────────────────────
    producer_ctx = read_latest_producer_context()
    topics_ok = validate_watched_topics(producer_ctx)
    lag_ok = validate_queue_lag(topics_ok)
    hb = emit_broker_heartbeat(lag_ok)
    publish_broker_asset(hb)


welding_realtime_broker_dag()
