"""
realtime.py
===========
new_src FileWatcher 기반 실험 결과를 CSV에서 읽는 엔드포인트 모음.
PostgreSQL 불필요. storage/metrics/realtime_experiment/*.csv → API 제공.

기존 routes(quality, pipeline 등)와 병존:
  - 기존 routes: PostgreSQL 필요 (Docker 기반 실험)
  - 이 파일  : CSV 파일 필요 (new_src 로컬 실험)
"""
from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

router = APIRouter(prefix="/api/v1/realtime", tags=["realtime (new_src)"])

ROOT_DIR = Path(__file__).resolve().parents[2]
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
HEARTBEAT_LOG = ROOT_DIR / "storage" / "logs" / "airflow_heartbeat.jsonl"
WATCHED_DIR = ROOT_DIR / "new_src" / "watched"
PROGRESS_FILE = ROOT_DIR / "storage" / "logs" / "consumer_progress.jsonl"
PID_FILE = ROOT_DIR / "storage" / "logs" / "experiment.pid"
EXPERIMENT_LOG = ROOT_DIR / "storage" / "logs" / "realtime_experiment.log"


# ── 내부 헬퍼 ─────────────────────────────────────────────────────────────────

def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with csv_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["cpd_score"] = float(row["cpd_score"]) if row.get("cpd_score") else None
                row["channel"] = int(row["channel"]) if row.get("channel") else 0
                rows.append(dict(row))
    except Exception:
        pass
    return rows


def _all_csv_sorted() -> list[Path]:
    if not METRICS_DIR.exists():
        return []
    return sorted(METRICS_DIR.glob("*.csv"), key=lambda x: x.stat().st_mtime, reverse=True)


def _latest_csv() -> Path | None:
    csvs = _all_csv_sorted()
    return csvs[0] if csvs else None


def _latest_heartbeat(component: str | None = None, status: str | None = None) -> dict:
    if not HEARTBEAT_LOG.exists():
        return {}
    lines = HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            if component and rec.get("component") != component:
                continue
            if status and rec.get("status") != status:
                continue
            return rec
        except json.JSONDecodeError:
            continue
    return {}


def _queue_pending() -> int:
    """watched/ 폴더에 남아있는 CSV 파일 수 (= 미처리 메시지 수)."""
    if not WATCHED_DIR.exists():
        return 0
    return sum(1 for _ in WATCHED_DIR.rglob("*.csv"))


def _is_process_alive(pid: int) -> bool:
    """PID 가 현재 실행 중인지 확인 (Windows / Unix 공통)."""
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
                capture_output=True, text=True,
            )
            return str(pid) in result.stdout
        else:
            os.kill(pid, 0)
            return True
    except (subprocess.SubprocessError, OSError):
        return False


def _kill_process(pid: int) -> None:
    """실험 프로세스 트리 강제 종료."""
    try:
        if sys.platform == "win32":
            subprocess.run(
                ["taskkill", "/F", "/T", "/PID", str(pid)],
                capture_output=True,
            )
        else:
            import signal
            os.kill(pid, signal.SIGKILL)
    except (subprocess.SubprocessError, OSError):
        pass


def _cleanup_experiment_files(keep_csv: bool = False) -> None:
    """실험 중간/최종 결과물 삭제."""
    # watched/ 폴더 (queue lag 지표)
    if WATCHED_DIR.exists():
        shutil.rmtree(WATCHED_DIR, ignore_errors=True)
    # 결과 CSV (keep_csv=True 이면 보존)
    if not keep_csv and METRICS_DIR.exists():
        for f in METRICS_DIR.glob("*.csv"):
            try:
                f.unlink()
            except OSError:
                pass
    # 로그 파일들
    for logf in [HEARTBEAT_LOG, PROGRESS_FILE, EXPERIMENT_LOG]:
        try:
            logf.unlink(missing_ok=True)
        except OSError:
            pass
    # PID 파일
    try:
        PID_FILE.unlink(missing_ok=True)
    except OSError:
        pass


# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@router.get("/system/metrics")
def system_metrics() -> dict[str, Any]:
    """
    시스템 전체 + 실험 프로세스 CPU / 메모리 / 스레드 현황.
    psutil 미설치 시 error 필드만 반환.
    """
    result: dict[str, Any] = {
        "system": {},
        "experiment_process": {"running": False},
        "error": None,
    }
    try:
        import psutil  # type: ignore[import]

        # 시스템 전체
        cpu_pct = psutil.cpu_percent(interval=0.3)
        vm = psutil.virtual_memory()
        result["system"] = {
            "cpu_pct": cpu_pct,
            "cpu_count": psutil.cpu_count(logical=True),
            "memory_used_gb": round(vm.used / 1024 ** 3, 2),
            "memory_total_gb": round(vm.total / 1024 ** 3, 2),
            "memory_pct": vm.percent,
        }

        # 실험 프로세스
        if PID_FILE.exists():
            try:
                pid = int(PID_FILE.read_text().strip())
                if _is_process_alive(pid):
                    p = psutil.Process(pid)
                    proc_cpu = p.cpu_percent(interval=0.3)
                    proc_mem = p.memory_info().rss / 1024 ** 2
                    children = p.children(recursive=True)
                    child_mem = sum(
                        c.memory_info().rss for c in children
                        if c.is_running()
                    ) / 1024 ** 2
                    result["experiment_process"] = {
                        "running": True,
                        "pid": pid,
                        "cpu_pct": round(proc_cpu, 1),
                        "memory_mb": round(proc_mem, 1),
                        "child_memory_mb": round(child_mem, 1),
                        "threads": p.num_threads(),
                        "child_procs": len(children),
                    }
            except (ValueError, OSError):
                pass
    except ImportError:
        result["error"] = "psutil 미설치 — pip install psutil 후 API 재시작"
    except Exception as exc:
        result["error"] = str(exc)
    return result

@router.get("/latest")
def realtime_latest(
    limit: int = Query(default=100, ge=1, le=500),
) -> list[dict[str, Any]]:
    """최신 실험(동일 시간대 모든 채널 CSV)의 결과를 합산해 반환."""
    csvs = _all_csv_sorted()
    if not csvs:
        return []
    
    # 가장 최근 파일의 타임스탬프(또는 파일명 접두어)를 기준으로 
    # 같은 실험군에 속하는 모든 채널 파일 읽기
    latest_ts = csvs[0].name.split("_")[0] # 예: 20260515_123456
    
    all_rows: list[dict[str, Any]] = []
    for p in csvs:
        if p.name.startswith(latest_ts):
            all_rows.extend(_read_csv_rows(p))
    
    # 시간순 정렬 (최신순)
    all_rows.sort(key=lambda r: r.get("processed_at", ""), reverse=True)
    return all_rows[:limit]


@router.get("/history")
def realtime_history(
    limit: int = Query(default=3000, ge=1, le=10000),
) -> list[dict[str, Any]]:
    """모든 실험 CSV 결과를 합산해 최신 순으로 반환."""
    all_rows: list[dict[str, Any]] = []
    for p in _all_csv_sorted():
        all_rows.extend(_read_csv_rows(p))
    return all_rows[:limit]


@router.get("/progress")
def realtime_progress(
    total_batteries: int = Query(default=20, ge=1, description="실험 목표 배터리 수"),
) -> dict[str, Any]:
    """
    현재 실험 진행 현황. (모든 채널 파일 합산)
    """
    rows: list[dict] = realtime_latest(limit=5000)

    batteries = {r["battery_id"] for r in rows}
    drift = sum(1 for r in rows if r.get("quality_decision") == "drift")

    pct = round(len(batteries) / total_batteries * 100, 1) if total_batteries else 0.0

    hb = _latest_heartbeat()
    run_id = hb.get("run_id", "")

    # processed_at 기준 최신 처리 배터리 (시간 문자열 정렬)
    latest_row = max(rows, key=lambda r: r.get("processed_at", ""), default=None)
    pending = _queue_pending()

    return {
        "run_id": run_id,
        "processed_batteries": len(batteries),
        "processed_channels": len(rows),
        "total_batteries": total_batteries,
        "progress_pct": pct,
        "drift_count": drift,
        "normal_count": len(rows) - drift,
        "latest_product_id": latest_row["battery_id"] if latest_row else None,
        "latest_processed_at": latest_row["processed_at"] if latest_row else None,
        "queue_pending_files": pending,
        "kafka_lag_laser_a": pending // 2,
        "kafka_lag_laser_b": pending - pending // 2,
    }


@router.get("/runs")
def realtime_runs() -> list[dict[str, Any]]:
    """실험 CSV 목록과 실행별 통계 요약."""
    result: list[dict[str, Any]] = []
    for p in _all_csv_sorted():
        rows = _read_csv_rows(p)
        batteries = {r["battery_id"] for r in rows}
        drift = sum(1 for r in rows if r.get("quality_decision") == "drift")
        result.append({
            "filename": p.name,
            "rows": len(rows),
            "batteries": len(batteries),
            "drift_count": drift,
            "normal_count": len(rows) - drift,
            "modified_at": p.stat().st_mtime,
        })
    return result


@router.get("/heartbeat")
def realtime_heartbeat() -> dict[str, Any]:
    """최신 Airflow 하트비트 레코드 반환 (없으면 빈 dict)."""
    return _latest_heartbeat()


@router.get("/stages")
def realtime_stages() -> dict[str, Any]:
    """
    new_src 파이프라인 단계 상태를 하트비트 기반으로 반환.
    기존 /pipeline/stages (PostgreSQL stage_event) 의 new_src 대응.

    단계:
      PRODUCER_DONE   → DataFeeder + FileWatcher 완료
      BROKER_READY    → 큐 전달 검증 완료
      CONSUMER_DONE   → 드리프트 분석 + CSV 저장 완료
    """
    statuses: dict[str, dict] = {}
    if HEARTBEAT_LOG.exists():
        for line in HEARTBEAT_LOG.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                status = rec.get("status", "")
                statuses[status] = rec
            except json.JSONDecodeError:
                continue

    def _stage(key: str, label: str) -> dict:
        rec = statuses.get(key, {})
        return {
            "name": key,
            "label": label,
            "status": "SUCCESS" if rec else "PENDING",
            "ended_at": rec.get("emitted_at"),
            "error_message": None,
        }

    run_id = (statuses.get("PRODUCER_DONE") or statuses.get("BROKER_READY") or {}).get("run_id")

    return {
        "run_id": run_id,
        "stages": [
            _stage("PRODUCER_DONE", "DataFeeder + FileWatcher 완료"),
            _stage("BROKER_READY",  "큐 검증 완료 (Broker)"),
            _stage("CONSUMER_PROCESSED", "드리프트 분석 완료 (Consumer)"),
        ],
    }


@router.get("/broker/state")
def broker_state() -> dict[str, Any]:
    """
    watched/ 폴더에 대기 중인 파일 목록 = 브로커(Kafka 대체) 미처리 메시지.
    Consumer가 처리 완료 후 파일을 삭제하므로 잔여 파일 수 = queue lag.
    """
    import re as _re
    files: list[dict[str, Any]] = []
    if WATCHED_DIR.exists():
        for f in sorted(WATCHED_DIR.rglob("*.csv"), key=lambda x: x.name):
            m = _re.search(r"battery_(\d+)", f.stem)
            bid = int(m.group(1)) if m else -1
            channel = "laser_a" if "laser_a" in f.name else "laser_b"
            line_id = f.parent.name
            try:
                size_kb = round(f.stat().st_size / 1024, 1)
            except OSError:
                size_kb = 0.0
            files.append({
                "battery_id": bid,
                "channel": channel,
                "line_id": line_id,
                "size_kb": size_kb,
                "filename": f.name,
            })

    la = sum(1 for x in files if x["channel"] == "laser_a")
    lb = sum(1 for x in files if x["channel"] == "laser_b")
    return {
        "pending_count": len(files),
        "laser_a_count": la,
        "laser_b_count": lb,
        "files": files,
    }


@router.get("/consumer/progress")
def consumer_progress() -> dict[str, Any]:
    """
    각 Consumer의 현재 처리 단계 + 처리율(채널/분) 반환.
    - consumers: consumer_id → 최신 진행 레코드
    - done_batteries: 처리 완료된 배터리 ID 목록
    - throughput_per_min: 최근 60초 기준 완료 채널 수/분
    - first_ts / last_ts: 실험 시작·최근 시각
    """
    from datetime import timezone as _tz  # 순환 import 방지

    if not PROGRESS_FILE.exists():
        return {
            "consumers": {},
            "done_batteries": [],
            "throughput_per_min": 0.0,
            "first_ts": None,
            "last_ts": None,
        }

    latest: dict[str, dict] = {}
    done: set[str] = set()
    done_ts: list[str] = []   # "완료" 타임스탬프 (ISO)
    all_ts: list[str] = []

    for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            cid = str(rec.get("consumer_id", "?"))
            latest[cid] = rec
            ts_str = rec.get("ts", "")
            if ts_str:
                all_ts.append(ts_str)
            if rec.get("stage") == "완료":
                done.add(str(rec.get("battery_id", "")))
                if ts_str:
                    done_ts.append(ts_str)
        except json.JSONDecodeError:
            continue

    # 처리율: 최근 60초 안에 완료된 채널 수 → 분당 환산
    throughput = 0.0
    try:
        now = datetime.now(_tz.utc)
        recent = sum(
            1 for ts in done_ts
            if (now - datetime.fromisoformat(ts)).total_seconds() <= 60
        )
        throughput = round(recent * 1.0, 1)   # channels/60s → display as-is
    except Exception:
        pass

    first_ts = min(all_ts) if all_ts else None
    last_ts  = max(all_ts) if all_ts else None

    return {
        "consumers": latest,
        "done_batteries": sorted(done),
        "throughput_per_min": throughput,   # 최근 60초 완료 채널 수
        "first_ts": first_ts,
        "last_ts": last_ts,
    }


@router.get("/experiment/status")
def experiment_status() -> dict[str, Any]:
    """실험 프로세스 실행 상태 반환."""
    if not PID_FILE.exists():
        return {"running": False, "pid": None}

    try:
        pid = int(PID_FILE.read_text().strip())
    except (ValueError, OSError):
        return {"running": False, "pid": None}

    alive = _is_process_alive(pid)
    if not alive:
        # 프로세스가 종료됐으면 PID 파일 정리
        try:
            PID_FILE.unlink(missing_ok=True)
        except OSError:
            pass
    return {"running": alive, "pid": pid if alive else None}


@router.post("/experiment/start")
def experiment_start(
    batteries: int   = Query(default=20,  ge=1,   le=500,  description="총 배터리 수"),
    lines: int       = Query(default=2,   ge=1,   le=10,    description="생산라인 수"),
    consumers: int   = Query(default=2,   ge=2,   le=8,    description="컨슈머 수 (짝수)"),
    interval: float  = Query(default=3.0, ge=0.5, le=30.0, description="DataFeeder 용접 주기(초)"),
    la_delay: float  = Query(default=0.0, ge=0.0, le=30.0, description="laser_a 처리 지연(초)"),
    lb_delay: float  = Query(default=0.0, ge=0.0, le=30.0, description="laser_b 처리 지연(초)"),
) -> dict[str, Any]:
    """
    실험 프로세스를 백그라운드로 시작.
    이미 실행 중이면 409 반환.
    """
    # 이미 실행 중이면 중복 시작 방지
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if _is_process_alive(pid):
                raise HTTPException(status_code=409, detail=f"이미 실험이 실행 중입니다 (PID={pid})")
        except (ValueError, OSError):
            pass

    if consumers % 2 != 0:
        raise HTTPException(status_code=422, detail="consumers는 짝수여야 합니다")

    # 이전 실험 잔여물 정리 (CSV는 Stop 전까지 보존하지 않으므로 삭제)
    _cleanup_experiment_files(keep_csv=False)

    script = ROOT_DIR / "new_src" / "run_realtime_experiment.py"
    (ROOT_DIR / "storage" / "logs").mkdir(parents=True, exist_ok=True)

    extra_kwargs: dict[str, Any] = {}
    if sys.platform == "win32":
        extra_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP  # type: ignore[assignment]
    else:
        extra_kwargs["start_new_session"] = True

    with open(EXPERIMENT_LOG, "w", encoding="utf-8") as log_file:
        proc = subprocess.Popen(
            [
                sys.executable, str(script),
                "--batteries", str(batteries),
                "--lines", str(lines),
                "--consumers", str(consumers),
                "--interval", str(interval),
                "--la-delay", str(la_delay),
                "--lb-delay", str(lb_delay),
            ],
            cwd=str(ROOT_DIR),
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
            **extra_kwargs,
        )
    # log_file is closed here; child process retains its own fd copy
    PID_FILE.write_text(str(proc.pid), encoding="utf-8")

    return {
        "status": "started",
        "pid": proc.pid,
        "params": {
            "batteries": batteries,
            "lines": lines,
            "consumers": consumers,
            "interval": interval,
            "la_delay": la_delay,
            "lb_delay": lb_delay,
        },
    }


@router.post("/experiment/stop")
def experiment_stop() -> dict[str, Any]:
    """
    실험 프로세스를 강제 종료하고 모든 결과물(중간 + 최종)을 삭제.
    실행 중이 아니어도 정리 작업은 수행.
    """
    killed_pid: int | None = None

    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            if _is_process_alive(pid):
                _kill_process(pid)
                killed_pid = pid
        except (ValueError, OSError):
            pass

    _cleanup_experiment_files(keep_csv=False)

    return {
        "status": "stopped",
        "killed_pid": killed_pid,
        "cleaned": True,
    }


@router.get("/logs")
def realtime_logs(
    consumer_id: int = Query(default=1, ge=1, le=2),
    lines: int = Query(default=40, ge=5, le=200),
) -> dict[str, Any]:
    """
    실험 프로세스 출력 로그 마지막 N줄 반환.
    기존 /pipeline/logs (Docker exec) 의 new_src 대응.
    실험 stdout을 파일로 리다이렉트했을 때 읽을 수 있음.
    """
    log_candidates = [
        ROOT_DIR / "storage" / "logs" / f"realtime_consumer_{consumer_id}.log",
        ROOT_DIR / "storage" / "logs" / "realtime_experiment.log",
    ]
    for log_path in log_candidates:
        if log_path.exists():
            try:
                all_lines = log_path.read_text(encoding="utf-8").splitlines()
                content = "\n".join(all_lines[-lines:])
                return {
                    "consumer_id": consumer_id,
                    "channel": "laser_a" if consumer_id % 2 == 1 else "laser_b",
                    "log_path": str(log_path),
                    "lines": lines,
                    "content": content,
                }
            except Exception as exc:
                return {"consumer_id": consumer_id, "content": f"(로그 읽기 오류: {exc})"}

    return {
        "consumer_id": consumer_id,
        "channel": "laser_a" if consumer_id % 2 == 1 else "laser_b",
        "log_path": "(없음)",
        "lines": lines,
        "content": (
            "로그 파일 없음 — run_realtime_sim.sh 실행 시\n"
            "'--no-ui' 없이 실행하거나 stdout을 파일로 리다이렉트하세요.\n"
            "예: bash new_src/run_realtime_sim.sh > storage/logs/realtime_experiment.log 2>&1"
        ),
    }
