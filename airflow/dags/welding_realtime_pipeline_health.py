"""
welding_realtime_pipeline_health.py
=====================================
new_src/ 아키텍처 전용 — 5분 주기 파이프라인 헬스체크 DAG.
Docker/Kafka/PostgreSQL 없이 로컬 파일 시스템만 사용.

파이프라인 구조 (감시 대상):
  DataFeeder → new_src/watched/LINE_XX/ → FileWatcher → queue → Consumer
                                                                    ↓
                                               storage/metrics/realtime_experiment/*.csv

점검 항목:
  1. Consumer 처리율  : 최근 HEALTH_WINDOW_MIN 분 내 신규 result CSV 존재 여부
  2. 큐 적체(backlog) : watched/ 폴더에 STALE_THRESHOLD_SEC 초 이상 오래된 CSV 파일
  3. 처리 누적 건수   : METRICS_DIR 내 오늘자 CSV 총 행 수

흐름:
  check_pipeline_health (branch)
    ├── [healthy]   → log_healthy_heartbeat
    └── [unhealthy] → alert_unhealthy_pipeline

로그:
  storage/logs/airflow_heartbeat.jsonl (JSONL, append 기록)
  — PostgreSQL 불필요, 파일 기반으로 운영 이벤트 추적

환경변수:
  WELDING_ROOT_DIR        : 프로젝트 루트 경로 (기본: D:/metacode_battery_drfit/welding-kafka-submission)
  REALTIME_HEALTH_WINDOW  : Consumer 생존 판단 윈도우(분, 기본: 10)
  REALTIME_STALE_THRESHOLD: 큐 적체 판단 기준(초, 기본: 120)
  ALERT_WEBHOOK_URL       : 장애 알림 웹훅 URL (선택)

Airflow 역할 (중요):
  이 DAG는 DataFeeder/FileWatcher/Consumer를 "실행"하지 않는다.
  파이프라인은 run_realtime_sim.sh 로 독립 실행된다.
  Airflow는 파이프라인이 정상 동작 중인지 "감시"만 한다.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from welding_realtime_assets import REALTIME_PIPELINE_HEALTHY_ASSET

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
HEALTH_WINDOW_MIN = int(os.getenv("REALTIME_HEALTH_WINDOW", "10"))
STALE_THRESHOLD_SEC = int(os.getenv("REALTIME_STALE_THRESHOLD", "120"))
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "").strip()


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    """운영 이벤트를 JSONL 파일에 append 기록 (PostgreSQL 대체)."""
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _send_webhook_alert(title: str, details: dict) -> None:
    """선택적 웹훅 알림 (Slack, PagerDuty 등)."""
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
        log.warning("웹훅 알림 전송 실패: %s", exc)


# ── 헬스체크 로직 ─────────────────────────────────────────────────────────────

def _check_consumer_throughput(window_min: int) -> tuple[bool, int]:
    """
    최근 window_min 분 내 result CSV가 생성/수정되었는지 확인한다.
    Consumer가 살아있고 처리를 하고 있으면 True.
    """
    if not METRICS_DIR.exists():
        return False, 0
    cutoff = time.time() - window_min * 60
    recent_files = [
        p for p in METRICS_DIR.glob("**/*.csv")
        if p.stat().st_mtime > cutoff
    ]
    return len(recent_files) > 0, len(recent_files)


def _check_queue_backlog() -> tuple[bool, list[str]]:
    """
    watched/ 폴더에 STALE_THRESHOLD_SEC 초 이상 오래된 CSV 파일을 탐색한다.
    오래된 파일이 있으면 FileWatcher → Consumer 처리가 지연 중임을 의미한다.
    """
    if not WATCHED_DIR.exists():
        # 폴더 없음 = 파이프라인 미시작. backlog 없음으로 처리 (경고 없음).
        return True, []
    cutoff = time.time() - STALE_THRESHOLD_SEC
    stale_files = []
    for csv_path in WATCHED_DIR.rglob("*.csv"):
        try:
            if csv_path.stat().st_mtime < cutoff:
                stale_files.append(str(csv_path.relative_to(ROOT_DIR)))
        except OSError:
            continue
    return len(stale_files) == 0, stale_files


def _count_today_results() -> int:
    """오늘 날짜 prefix의 result CSV 총 행 수를 반환한다."""
    if not METRICS_DIR.exists():
        return 0
    today = datetime.now().strftime("%Y%m%d")
    total = 0
    for csv_path in METRICS_DIR.glob(f"realtime_{today}*.csv"):
        try:
            lines = csv_path.read_text(encoding="utf-8").splitlines()
            total += max(len(lines) - 1, 0)  # 헤더 제외
        except OSError:
            continue
    return total


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_pipeline_health",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "realtime", "monitoring"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    doc_md="""
## welding_realtime_pipeline_health

**아키텍처**: new_src/ FileWatcher 기반 실시간 파이프라인

**목적**: 5분 주기로 파이프라인이 정상 작동 중인지 감시한다.

**Airflow 역할**
- 파이프라인을 "실행"하지 않는다 (run_realtime_sim.sh 가 독립 실행)
- 파이프라인 상태를 "감시"하고 이상 시 알림

**점검 항목**
| 항목 | 기준 |
|---|---|
| Consumer 처리율 | 최근 10분 내 result CSV 존재 여부 |
| 큐 적체 | watched/ 폴더에 2분 이상 오래된 CSV 파일 |

**로그**: `storage/logs/airflow_heartbeat.jsonl`

**환경변수**
- `REALTIME_HEALTH_WINDOW`: Consumer 생존 윈도우(분, 기본 10)
- `REALTIME_STALE_THRESHOLD`: 큐 적체 판단 기준(초, 기본 120)
- `ALERT_WEBHOOK_URL`: 장애 알림 웹훅 (선택)
    """,
)
def welding_realtime_pipeline_health_dag():

    @task.branch()
    def check_pipeline_health() -> str:
        """
        Consumer 처리율과 큐 적체를 동시에 검사한다.
        둘 다 정상이면 log_healthy_heartbeat, 하나라도 이상이면 alert_unhealthy_pipeline.
        """
        consumer_ok, recent_count = _check_consumer_throughput(HEALTH_WINDOW_MIN)
        backlog_ok, stale_files = _check_queue_backlog()
        today_total = _count_today_results()
        is_healthy = consumer_ok and backlog_ok

        status = {
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "consumer_throughput_ok": consumer_ok,
            "recent_result_files": recent_count,
            "health_window_min": HEALTH_WINDOW_MIN,
            "backlog_ok": backlog_ok,
            "stale_file_count": len(stale_files),
            "stale_files_sample": stale_files[:5],
            "stale_threshold_sec": STALE_THRESHOLD_SEC,
            "today_processed_rows": today_total,
            "is_healthy": is_healthy,
        }
        _write_heartbeat({"component": "airflow.realtime_health.check", **status})

        if is_healthy:
            log.info(
                "파이프라인 정상 — recent_files=%d, stale_files=0, today_rows=%d",
                recent_count, today_total,
            )
            return "log_healthy_heartbeat"
        else:
            reasons = []
            if not consumer_ok:
                reasons.append(f"Consumer 처리 없음 (최근 {HEALTH_WINDOW_MIN}분)")
            if not backlog_ok:
                reasons.append(f"큐 적체 {len(stale_files)}개 파일 ({STALE_THRESHOLD_SEC}초 초과)")
            log.warning("파이프라인 이상 — %s", " | ".join(reasons))
            return "alert_unhealthy_pipeline"

    @task(outlets=[REALTIME_PIPELINE_HEALTHY_ASSET])
    def log_healthy_heartbeat():
        """
        정상 상태를 heartbeat 파일에 기록하고 REALTIME_PIPELINE_HEALTHY_ASSET을 emit.
        → 이 Asset을 구독하는 다른 DAG가 있으면 자동 트리거된다.
        """
        today_total = _count_today_results()
        record = {
            "component": "airflow.realtime_health",
            "status": "healthy",
            "today_processed_rows": today_total,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_heartbeat(record)
        log.info("정상 heartbeat 기록 완료 — today_rows=%d", today_total)
        return record

    @task()
    def alert_unhealthy_pipeline():
        """
        이상 상태를 heartbeat 파일에 기록하고 웹훅 알림을 전송한다.

        대응 가이드:
          1. run_realtime_sim.sh 프로세스가 살아 있는지 확인
          2. new_src/watched/ 폴더의 파일 적체 여부 확인
          3. storage/metrics/realtime_experiment/ 에 최근 CSV가 있는지 확인
          4. 누락된 데이터가 있으면 welding_realtime_manual_backfill DAG 트리거
        """
        _, recent_count = _check_consumer_throughput(HEALTH_WINDOW_MIN)
        _, stale_files = _check_queue_backlog()

        details = {
            "recent_result_files": recent_count,
            "stale_file_count": len(stale_files),
            "stale_files_sample": stale_files[:5],
            "action_required": "run_realtime_sim.sh 상태 확인 또는 welding_realtime_manual_backfill 트리거",
        }
        record = {
            "component": "airflow.realtime_health",
            "status": "unhealthy",
            "details": details,
            "alerted_at": datetime.now(timezone.utc).isoformat(),
        }
        _write_heartbeat(record)
        log.error(
            "파이프라인 이상 알림 — recent=%d, stale=%d. 수동 점검 또는 백필 필요.",
            recent_count, len(stale_files),
        )
        _send_webhook_alert("welding realtime pipeline unhealthy", details)

    branch = check_pipeline_health()
    branch >> log_healthy_heartbeat()
    branch >> alert_unhealthy_pipeline()


welding_realtime_pipeline_health_dag()
