"""
welding_realtime_storage_cleanup.py
=====================================
new_src/ 아키텍처 전용 — 매일 03:00 스토리지 정리 DAG.
PostgreSQL/Docker 없이 로컬 파일 시스템 직접 정리.

정리 대상:
  1. new_src/watched/**/  : 처리 완료된 잔여 CSV (24시간 이상 오래된 파일)
     → Consumer가 처리한 후에도 남아 있는 watched/ 폴더 파일 제거
  2. storage/metrics/realtime_experiment/ : 7일 이상 오래된 result CSV
     → 오래된 실험 결과 정리, 최근 7일은 welding_realtime_daily_report가 참조
  3. storage/logs/airflow_heartbeat.jsonl : 로그 파일 로테이션
     → 10,000줄 초과 시 가장 오래된 절반 제거 (append-only 파일 크기 제어)

흐름:
  check_storage_status
    → clean_watched_folder
    → clean_old_metrics
    → rotate_heartbeat_log
    → report_cleanup_result

안전 장치:
  - dry_run=True 이면 대상 목록만 출력하고 실제 삭제 없음
  - watched/ 정리 시 현재 실행 중인 파일 (수정 시각 < STALE_THRESHOLD_SEC) 은 보호
  - 정리 결과를 heartbeat 파일에 기록

환경변수:
  WELDING_ROOT_DIR                : 프로젝트 루트 경로
  REALTIME_WATCHED_RETAIN_HOURS   : watched/ 보관 시간 (기본: 24)
  REALTIME_METRICS_RETAIN_DAYS    : result CSV 보관 일수 (기본: 7)
  REALTIME_HEARTBEAT_MAX_LINES    : heartbeat 로그 최대 줄 수 (기본: 10000)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
WATCHED_DIR = ROOT_DIR / "new_src" / "watched"
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
REPORTS_DIR = ROOT_DIR / "storage" / "reports"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"

# ── 보관 기준 ─────────────────────────────────────────────────────────────────
WATCHED_RETAIN_HOURS = int(os.getenv("REALTIME_WATCHED_RETAIN_HOURS", "24"))
METRICS_RETAIN_DAYS = int(os.getenv("REALTIME_METRICS_RETAIN_DAYS", "7"))
HEARTBEAT_MAX_LINES = int(os.getenv("REALTIME_HEARTBEAT_MAX_LINES", "10000"))


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _get_dir_size_mb(path: Path) -> float:
    """디렉터리 총 크기(MB) 반환."""
    try:
        total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
        return round(total / 1024 / 1024, 2)
    except Exception:
        return 0.0


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_storage_cleanup",
    schedule="0 3 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    params={
        "dry_run": Param(
            default=False,
            type="boolean",
            description="True면 삭제 없이 대상만 나열한다.",
        ),
    },
    tags=["welding", "realtime", "maintenance"],
    default_args={
        "owner": "welding-team",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    doc_md="""
## welding_realtime_storage_cleanup

**아키텍처**: new_src/ FileWatcher 기반 실시간 파이프라인

**목적**: 매일 03:00에 오래된 파일을 자동 정리하여 디스크 용량을 확보한다.

**정리 기준**
| 대상 | 경로 | 보관 기간 |
|---|---|---|
| watched/ 잔여 파일 | `new_src/watched/**/` | 24시간 |
| result CSV | `storage/metrics/realtime_experiment/` | 7일 |
| heartbeat 로그 | `storage/logs/airflow_heartbeat.jsonl` | 10,000줄 초과 시 절반 제거 |

**안전 장치**
- `dry_run=True`: 삭제 없이 대상만 나열
- watched/ 정리 시 24시간 미만 파일은 보호 (현재 처리 중일 수 있음)

**Airflow 역할**
운영 자동화 — 사람이 개입 없이 디스크를 관리한다.
    """,
)
def welding_realtime_storage_cleanup_dag():

    @task()
    def check_storage_status() -> dict:
        """
        정리 전 각 디렉터리 크기와 파일 수를 확인하고 기록한다.
        """
        from airflow.operators.python import get_current_context
        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        watched_size = _get_dir_size_mb(WATCHED_DIR) if WATCHED_DIR.exists() else 0.0
        metrics_size = _get_dir_size_mb(METRICS_DIR) if METRICS_DIR.exists() else 0.0
        heartbeat_lines = 0
        if HEARTBEAT_LOG.exists():
            try:
                heartbeat_lines = sum(1 for _ in HEARTBEAT_LOG.open(encoding="utf-8"))
            except Exception:
                pass

        status = {
            "dry_run": dry_run,
            "watched_dir_mb": watched_size,
            "metrics_dir_mb": metrics_size,
            "heartbeat_log_lines": heartbeat_lines,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
        log.info(
            "스토리지 현황 — watched=%.1fMB, metrics=%.1fMB, heartbeat=%d줄",
            watched_size, metrics_size, heartbeat_lines,
        )
        return status

    @task()
    def clean_watched_folder(storage_status: dict) -> dict:
        """
        new_src/watched/ 폴더에서 WATCHED_RETAIN_HOURS 시간 이상 오래된 CSV를 삭제한다.

        이 파일들은 FileWatcher가 queue로 보낸 뒤 남아있는 잔여 파일이다.
        Consumer가 처리를 완료한 후에도 watched/ 에는 파일이 남으므로 주기적으로 정리한다.
        """
        from airflow.operators.python import get_current_context
        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        if not WATCHED_DIR.exists():
            log.info("watched/ 폴더 없음 — 파이프라인 미시작")
            return {**storage_status, "watched_deleted": 0, "watched_candidates": 0}

        cutoff = time.time() - WATCHED_RETAIN_HOURS * 3600
        candidates = []
        for csv_path in WATCHED_DIR.rglob("*.csv"):
            try:
                if csv_path.stat().st_mtime < cutoff:
                    candidates.append(csv_path)
            except OSError:
                continue

        deleted = 0
        if candidates:
            if dry_run:
                log.info(
                    "dry_run=True, watched/ 삭제 생략. 대상 %d개: %s ...",
                    len(candidates), [p.name for p in candidates[:3]],
                )
            else:
                for p in candidates:
                    try:
                        p.unlink()
                        deleted += 1
                    except OSError as exc:
                        log.warning("파일 삭제 실패 %s: %s", p.name, exc)
                log.info("watched/ 잔여 파일 %d개 삭제 완료", deleted)
        else:
            log.info("삭제 대상 watched/ 파일 없음 (보관 기준: %dh)", WATCHED_RETAIN_HOURS)

        # 빈 하위 디렉터리 제거
        for sub in sorted(WATCHED_DIR.iterdir(), reverse=True):
            if sub.is_dir():
                try:
                    sub.rmdir()  # 비어 있어야 성공
                except OSError:
                    pass

        return {**storage_status, "watched_candidates": len(candidates), "watched_deleted": deleted}

    @task()
    def clean_old_metrics(prev_result: dict) -> dict:
        """
        storage/metrics/realtime_experiment/ 내 METRICS_RETAIN_DAYS일 이상 오래된 CSV를 삭제한다.

        최근 7일 데이터는 welding_realtime_daily_report DAG가 참조하므로 보호.
        더 오래된 데이터는 daily_report JSON에 이미 집계되었으므로 삭제 가능.
        """
        from airflow.operators.python import get_current_context
        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        if not METRICS_DIR.exists():
            log.info("metrics/ 폴더 없음")
            return {**prev_result, "metrics_deleted": 0, "metrics_candidates": 0}

        cutoff = time.time() - METRICS_RETAIN_DAYS * 86400
        candidates = []
        for csv_path in METRICS_DIR.glob("*.csv"):
            try:
                if csv_path.stat().st_mtime < cutoff:
                    candidates.append(csv_path)
            except OSError:
                continue
        # summary txt도 같이 정리
        for txt_path in METRICS_DIR.glob("*.txt"):
            try:
                if txt_path.stat().st_mtime < cutoff:
                    candidates.append(txt_path)
            except OSError:
                continue

        deleted = 0
        if candidates:
            if dry_run:
                log.info(
                    "dry_run=True, metrics/ 삭제 생략. 대상 %d개: %s ...",
                    len(candidates), [p.name for p in candidates[:3]],
                )
            else:
                for p in candidates:
                    try:
                        p.unlink()
                        deleted += 1
                    except OSError as exc:
                        log.warning("파일 삭제 실패 %s: %s", p.name, exc)
                log.info("오래된 result 파일 %d개 삭제 완료", deleted)
        else:
            log.info("삭제 대상 result 파일 없음 (보관 기준: %d일)", METRICS_RETAIN_DAYS)

        return {**prev_result, "metrics_candidates": len(candidates), "metrics_deleted": deleted}

    @task()
    def rotate_heartbeat_log(prev_result: dict) -> dict:
        """
        airflow_heartbeat.jsonl 파일이 HEARTBEAT_MAX_LINES 초과 시
        가장 오래된 절반을 제거한다 (tail rotation).

        append-only JSONL 파일이 무한히 커지는 것을 방지한다.
        """
        from airflow.operators.python import get_current_context
        dry_run = bool(get_current_context()["params"].get("dry_run", False))

        if not HEARTBEAT_LOG.exists():
            log.info("heartbeat 로그 파일 없음")
            return {**prev_result, "heartbeat_rotated": False, "heartbeat_lines_before": 0}

        try:
            lines = HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines(keepends=True)
        except Exception as exc:
            log.warning("heartbeat 로그 읽기 실패: %s", exc)
            return {**prev_result, "heartbeat_rotated": False, "heartbeat_lines_before": 0}

        lines_before = len(lines)
        rotated = False

        if lines_before > HEARTBEAT_MAX_LINES:
            keep_from = lines_before // 2  # 최신 절반만 유지
            lines_to_keep = lines[keep_from:]
            if dry_run:
                log.info(
                    "dry_run=True, heartbeat 로테이션 생략. %d줄 → %d줄",
                    lines_before, len(lines_to_keep),
                )
            else:
                HEARTBEAT_LOG.write_text("".join(lines_to_keep), encoding="utf-8")
                rotated = True
                log.info(
                    "heartbeat 로그 로테이션 완료: %d줄 → %d줄 (삭제: %d줄)",
                    lines_before, len(lines_to_keep), lines_before - len(lines_to_keep),
                )
        else:
            log.info("heartbeat 로그 정상 크기: %d줄 (최대: %d줄)", lines_before, HEARTBEAT_MAX_LINES)

        return {
            **prev_result,
            "heartbeat_lines_before": lines_before,
            "heartbeat_rotated": rotated,
        }

    @task()
    def report_cleanup_result(result: dict):
        """
        정리 결과를 요약하고 heartbeat 파일에 기록한다.
        """
        dry_run = result.get("dry_run", False)
        total_deleted = result.get("watched_deleted", 0) + result.get("metrics_deleted", 0)

        summary = {
            "component": "airflow.realtime_storage_cleanup",
            "status": "dry_run" if dry_run else "completed",
            "dry_run": dry_run,
            "watched_candidates": result.get("watched_candidates", 0),
            "watched_deleted": result.get("watched_deleted", 0),
            "metrics_candidates": result.get("metrics_candidates", 0),
            "metrics_deleted": result.get("metrics_deleted", 0),
            "heartbeat_rotated": result.get("heartbeat_rotated", False),
            "heartbeat_lines_before": result.get("heartbeat_lines_before", 0),
            "total_deleted": total_deleted,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }

        _write_heartbeat(summary)
        log.info(
            "스토리지 정리 완료 (dry_run=%s) — watched=%d개, metrics=%d개 삭제",
            dry_run, result.get("watched_deleted", 0), result.get("metrics_deleted", 0),
        )

    # ── 의존성 연결 ──────────────────────────────────────────────────────────
    status = check_storage_status()
    watched = clean_watched_folder(status)
    metrics = clean_old_metrics(watched)
    rotated = rotate_heartbeat_log(metrics)
    report_cleanup_result(rotated)


welding_realtime_storage_cleanup_dag()
