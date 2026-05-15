"""
welding_realtime_daily_report.py
==================================
new_src/ 아키텍처 전용 — 매일 01:00 일별 드리프트 리포트 DAG.
PostgreSQL 없이 로컬 CSV 파일에서 직접 집계한다.

데이터 소스:
  storage/metrics/realtime_experiment/realtime_{YYYYMMDD}*.csv

리포트 출력:
  storage/reports/daily_drift_report_{YYYYMMDD}.json

흐름:
  check_data_exists (short_circuit guard)
    → aggregate_from_csv
    → detect_drift_anomaly
    → write_daily_report (→ REALTIME_DAILY_REPORT_ASSET emit)

날짜 기준:
  ds  = DAG 실행일 (e.g. 2026-05-15)
  target_date = ds - 1일 = 전일 (집계 대상)

가드 (short_circuit):
  전일 result CSV 행 수 < MIN_EXPECTED_ROWS 이면 하위 Task 전체 스킵.
  → 파이프라인이 멈췄거나 데이터 없는 날은 빈 리포트 방지.
  → welding_realtime_manual_backfill 로 소급 처리 후 재실행 권장.

Airflow 역할:
  파이프라인이 하루 동안 생성한 CSV 결과를 사후 집계.
  드리프트 이상 탐지 및 운영팀 리포트 제공.
  실시간 데이터 처리 자체는 Consumer가 담당.
"""

from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.decorators import dag, task
from welding_realtime_assets import REALTIME_DAILY_REPORT_ASSET

log = logging.getLogger(__name__)

# ── 경로 설정 ─────────────────────────────────────────────────────────────────
ROOT_DIR = Path(os.getenv(
    "WELDING_ROOT_DIR",
    "D:/metacode_battery_drfit/welding-kafka-submission",
))
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
REPORTS_DIR = ROOT_DIR / "storage" / "reports"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"

# ── 임계값 ────────────────────────────────────────────────────────────────────
MIN_EXPECTED_ROWS = int(os.getenv("REALTIME_MIN_EXPECTED_ROWS", "2"))
DRIFT_RATE_THRESHOLD = float(os.getenv("REALTIME_DRIFT_RATE_THRESHOLD", "0.3"))
MAX_CPD_ALERT_THRESHOLD = float(os.getenv("REALTIME_MAX_CPD_ALERT_THRESHOLD", "0.5"))


# ── 공통 유틸 ─────────────────────────────────────────────────────────────────

def _write_heartbeat(record: dict) -> None:
    HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
    with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def _load_day_results(target_yyyymmdd: str) -> list[dict]:
    """
    target_yyyymmdd (YYYYMMDD) 에 해당하는 result CSV 파일 전체를 읽어 행 목록 반환.
    파일명 패턴: realtime_{YYYYMMDD}*.csv
    """
    rows: list[dict] = []
    if not METRICS_DIR.exists():
        return rows
    for csv_path in METRICS_DIR.glob(f"realtime_{target_yyyymmdd}*.csv"):
        try:
            with open(csv_path, encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    rows.append(row)
            log.debug("CSV 로드: %s (%d행)", csv_path.name, len(rows))
        except Exception as exc:
            log.warning("CSV 읽기 실패 %s: %s", csv_path.name, exc)
    return rows


def _safe_float(val) -> float | None:
    try:
        return float(val) if val not in (None, "", "None") else None
    except (ValueError, TypeError):
        return None


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="welding_realtime_daily_report",
    schedule="0 1 * * *",
    start_date=datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "realtime", "report"],
    default_args={
        "owner": "welding-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md="""
## welding_realtime_daily_report

**아키텍처**: new_src/ FileWatcher 기반 실시간 파이프라인

**목적**: 매일 01:00에 전일 용접 드리프트 결과를 CSV에서 집계하고 리포트를 생성한다.

**날짜 기준**
- `ds` = DAG 실행일 (e.g. 2026-05-15)
- `target_date` = ds - 1일 = 전일 집계 대상 (e.g. 20260514)

**데이터 소스**
```
storage/metrics/realtime_experiment/realtime_{YYYYMMDD}*.csv
```

**출력**
```
storage/reports/daily_drift_report_{YYYYMMDD}.json
```

**가드 (short_circuit)**
전일 데이터 < MIN_EXPECTED_ROWS 이면 하위 Task 전체 스킵.
누락 데이터는 `welding_realtime_manual_backfill` DAG로 소급 처리.

**이상 탐지 기준**
- drift_rate > REALTIME_DRIFT_RATE_THRESHOLD (기본 30%)
- max_cpd_score > REALTIME_MAX_CPD_ALERT_THRESHOLD (기본 0.5)

**Airflow 역할 (핵심)**
Consumer가 실시간으로 처리한 결과를 사후에 집계하는 감시자.
데이터 처리 자체는 Consumer(Spark Streaming 역할)가 담당.
    """,
)
def welding_realtime_daily_report_dag():

    @task.short_circuit()
    def check_data_exists(ds=None) -> bool:
        """
        전일(target_date) result CSV 행 수가 MIN_EXPECTED_ROWS 이상인지 확인.
        미만이면 False → 하위 Task 전체 스킵.
        """
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y%m%d")

        rows = _load_day_results(target_date)
        count = len(rows)
        log.info(
            "전일(%s) result CSV 행 수: %d (최소 요구: %d)",
            target_date, count, MIN_EXPECTED_ROWS,
        )
        if count < MIN_EXPECTED_ROWS:
            log.warning(
                "데이터 부족(%d < %d) → daily_report 스킵. "
                "welding_realtime_manual_backfill DAG로 소급 처리 권장.",
                count, MIN_EXPECTED_ROWS,
            )
            return False
        return True

    @task()
    def aggregate_from_csv(ds=None) -> dict:
        """
        전일 result CSV를 읽어 집계 통계를 산출한다.

        집계 항목:
          - total / drift / normal 건수 및 비율
          - cpd_score 평균, 최댓값
          - 라인별(line_id) 집계
          - 채널별(channel_name) 집계
        """
        target_date = (
            datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y%m%d")

        rows = _load_day_results(target_date)
        total = len(rows)

        drift_rows = [r for r in rows if r.get("quality_decision") == "drift"]
        normal_rows = [r for r in rows if r.get("quality_decision") == "normal"]

        cpd_scores = [s for s in (_safe_float(r.get("cpd_score")) for r in rows) if s is not None]

        # 라인별 집계
        by_line: dict[str, dict] = {}
        for r in rows:
            lid = r.get("line_id", "unknown")
            dec = r.get("quality_decision", "unknown")
            if lid not in by_line:
                by_line[lid] = {"total": 0, "drift": 0, "normal": 0}
            by_line[lid]["total"] += 1
            if dec in ("drift", "normal"):
                by_line[lid][dec] += 1

        # 채널별 집계
        by_channel: dict[str, dict] = {}
        for r in rows:
            ch = r.get("channel_name", "unknown")
            dec = r.get("quality_decision", "unknown")
            if ch not in by_channel:
                by_channel[ch] = {"total": 0, "drift": 0, "normal": 0}
            by_channel[ch]["total"] += 1
            if dec in ("drift", "normal"):
                by_channel[ch][dec] += 1

        # 드리프트 세그먼트 수집 (있는 경우)
        drift_segment_counts = []
        for r in drift_rows:
            ds_val = r.get("drift_segments", "")
            if "/" in str(ds_val):
                try:
                    numerator = int(str(ds_val).split("/")[0])
                    drift_segment_counts.append(numerator)
                except (ValueError, IndexError):
                    pass

        stats = {
            "target_date": target_date,
            "total": total,
            "drift": len(drift_rows),
            "normal": len(normal_rows),
            "drift_rate": round(len(drift_rows) / total, 4) if total else 0.0,
            "avg_cpd_score": round(sum(cpd_scores) / len(cpd_scores), 6) if cpd_scores else None,
            "max_cpd_score": round(max(cpd_scores), 6) if cpd_scores else None,
            "min_cpd_score": round(min(cpd_scores), 6) if cpd_scores else None,
            "avg_drift_segments": (
                round(sum(drift_segment_counts) / len(drift_segment_counts), 2)
                if drift_segment_counts else None
            ),
            "by_line": by_line,
            "by_channel": by_channel,
        }

        log.info(
            "집계 완료 — total=%d, drift=%d (%.1f%%), avg_cpd=%.4f",
            total, len(drift_rows), stats["drift_rate"] * 100,
            stats["avg_cpd_score"] or 0,
        )
        return stats

    @task()
    def detect_drift_anomaly(stats: dict) -> dict:
        """
        드리프트 이상 여부를 판정한다.

        판정 기준:
          - drift_rate > DRIFT_RATE_THRESHOLD (기본 30%)
          - max_cpd_score > MAX_CPD_ALERT_THRESHOLD (기본 0.5)

        두 기준 중 하나라도 초과하면 anomaly_detected=True.
        라인별로 드리프트 집중 라인을 식별해 worst_lines에 기록.
        """
        drift_rate = stats.get("drift_rate", 0.0)
        max_cpd = stats.get("max_cpd_score") or 0.0

        rate_exceeded = drift_rate > DRIFT_RATE_THRESHOLD
        cpd_exceeded = max_cpd > MAX_CPD_ALERT_THRESHOLD
        anomaly = rate_exceeded or cpd_exceeded

        # 드리프트 집중 라인 식별
        by_line = stats.get("by_line", {})
        worst_lines = sorted(
            [
                {"line_id": lid, "drift": v["drift"], "total": v["total"],
                 "drift_rate": round(v["drift"] / v["total"], 4) if v["total"] else 0}
                for lid, v in by_line.items()
                if v["drift"] > 0
            ],
            key=lambda x: x["drift_rate"],
            reverse=True,
        )

        stats.update({
            "anomaly_detected": anomaly,
            "drift_rate_exceeded": rate_exceeded,
            "cpd_score_exceeded": cpd_exceeded,
            "drift_rate_threshold": DRIFT_RATE_THRESHOLD,
            "max_cpd_alert_threshold": MAX_CPD_ALERT_THRESHOLD,
            "worst_lines": worst_lines[:3],
        })

        if anomaly:
            reasons = []
            if rate_exceeded:
                reasons.append(f"drift_rate={drift_rate:.1%} > 임계값 {DRIFT_RATE_THRESHOLD:.1%}")
            if cpd_exceeded:
                reasons.append(f"max_cpd={max_cpd:.4f} > 임계값 {MAX_CPD_ALERT_THRESHOLD}")
            log.warning(
                "드리프트 이상 감지! date=%s — %s | worst_lines=%s",
                stats["target_date"], " | ".join(reasons), worst_lines[:2],
            )
        else:
            log.info(
                "품질 정상 — date=%s, drift_rate=%.1f%%, max_cpd=%.4f",
                stats["target_date"], drift_rate * 100, max_cpd,
            )
        return stats

    @task(outlets=[REALTIME_DAILY_REPORT_ASSET])
    def write_daily_report(stats: dict):
        """
        집계 결과를 JSON 리포트 파일로 저장하고 heartbeat를 기록한다.
        REALTIME_DAILY_REPORT_ASSET을 emit해 다운스트림 DAG 트리거 가능.

        출력 경로:
          storage/reports/daily_drift_report_{YYYYMMDD}.json
        """
        target_date = stats["target_date"]
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        report = {
            **stats,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generated_by": "airflow.welding_realtime_daily_report",
            "data_source": str(METRICS_DIR / f"realtime_{target_date}*.csv"),
        }

        report_path = REPORTS_DIR / f"daily_drift_report_{target_date}.json"
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        _write_heartbeat({
            "component": "airflow.realtime_daily_report",
            "target_date": target_date,
            "total": stats["total"],
            "drift": stats["drift"],
            "drift_rate": stats["drift_rate"],
            "anomaly_detected": stats.get("anomaly_detected", False),
            "report_path": str(report_path),
            "generated_at": report["generated_at"],
        })

        log.info(
            "리포트 저장 완료: %s (total=%d, drift=%d, anomaly=%s)",
            report_path.name, stats["total"], stats["drift"], stats.get("anomaly_detected"),
        )

    # ── 의존성 연결 ──────────────────────────────────────────────────────────
    guard = check_data_exists()
    aggregated = aggregate_from_csv()
    guard >> aggregated
    with_anomaly = detect_drift_anomaly(aggregated)
    write_daily_report(with_anomaly)


welding_realtime_daily_report_dag()
