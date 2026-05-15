"""
실무 유사 실시간 실험
=====================
실제 레이저 용접 환경 시뮬레이션:

  DataFeeder      : 10초마다 배터리 CSV 2개(laser_a + laser_b)를
                    watched/ 폴더에 복사
                    → 실제 용접 센서가 파일을 생성하는 상황

  FileWatcherProducer : watched/ 폴더를 지속 모니터링
                        새 파일 감지 → Kafka 큐(queue)에 전달
                        → Kafka Producer 역할. 개수 모름, 계속 감시

  Consumer        : 큐에서 꺼내 drift 분석 → 결과 출력/저장
                    → Spark Streaming 역할. 도착하는 즉시 처리

차이점:
  기존 실험 : --batteries 20 → 개수 정해놓고 끝나면 멈춤
  이 실험   : 폴더 감시, 파일 올 때마다 처리, Ctrl+C로 생산라인 종료

실행:
  uv run python new_src/run_realtime_experiment.py
  uv run python new_src/run_realtime_experiment.py --interval 5 --lines 2 --consumers 2
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import queue
import re
import shutil
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from spark_batch import build_segment_rows


# ── 경로 설정 ─────────────────────────────────────────────────────────────────

def _resolve_source_base() -> Path:
    for c in [
        ROOT_DIR / "data" / "20220417",                                          # 프로젝트 내 data/
        Path("D:/metacode_battery_drfit/data_runtime_by_channel/20220417"),      # 외부 경로 (Windows)
        Path("/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),      # Git Bash
        Path("/mnt/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),  # WSL
    ]:
        if (c / "laser_a").is_dir():
            return c
    raise FileNotFoundError("소스 데이터 디렉터리를 찾을 수 없습니다.")


SOURCE_BASE = _resolve_source_base()
WATCHED_DIR = ROOT_DIR / "new_src" / "watched"
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "realtime_experiment"
PROGRESS_FILE = ROOT_DIR / "storage" / "logs" / "consumer_progress.jsonl"

# CSV 컬럼 순서 (Consumer 간 공유)
CSV_FIELDNAMES = [
    "battery_id", "line_id", "channel", "channel_name",
    "quality_decision", "cpd_score", "drift_segments", "processed_at",
]

_progress_lock = threading.Lock()   # PROGRESS_FILE 쓰기용
_csv_lock = threading.Lock()        # 결과 CSV append 쓰기용


def _write_progress(data: dict) -> None:
    """Consumer 진행 상황을 JSONL 파일에 원자적으로 기록."""
    try:
        with _progress_lock:
            with PROGRESS_FILE.open("a", encoding="utf-8") as f:
                f.write(json.dumps(data, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _append_csv(row: dict, csv_path: Path) -> None:
    """결과 한 건을 CSV에 즉시 추가 (실험 중 실시간 조회 지원)."""
    try:
        with _csv_lock:
            with open(csv_path, "a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
                writer.writerow(row)
    except OSError:
        pass


# ── 메시지 구조 (Kafka 메시지 역할) ───────────────────────────────────────────

class Message:
    __slots__ = ("file_path", "battery_id", "channel", "line_id", "arrived_at")

    def __init__(self, file_path: Path, battery_id: int, channel: int, line_id: str):
        self.file_path = file_path
        self.battery_id = battery_id
        self.channel = channel          # 0=laser_b, 1=laser_a
        self.line_id = line_id
        self.arrived_at = time.monotonic()


# ── DataFeeder : 생산라인 시뮬레이터 ─────────────────────────────────────────
#    실제 환경 : 레이저 용접 완료 → 센서가 watched/ 에 파일 생성
#    시뮬레이션 : 기존 배터리 CSV를 watched/ 에 복사

class DataFeeder(threading.Thread):
    def __init__(
        self,
        line_id: str,
        battery_ids: list[int],
        interval_sec: float,
        stop_event: threading.Event,
    ):
        super().__init__(name=f"DataFeeder-{line_id}", daemon=True)
        self.line_id = line_id
        self.battery_ids = battery_ids
        self.interval_sec = interval_sec
        self.stop_event = stop_event
        self.sent = 0

    def run(self) -> None:
        line_dir = WATCHED_DIR / self.line_id
        line_dir.mkdir(parents=True, exist_ok=True)

        for bid in self.battery_ids:
            if self.stop_event.is_set():
                break

            # 용접 1회 = laser_a + laser_b 파일 2개 생성
            src_a = SOURCE_BASE / "laser_a" / f"20220417_battery_{bid}_laser_a.csv"
            src_b = SOURCE_BASE / "laser_b" / f"20220417_battery_{bid}_laser_b.csv"

            ts = datetime.now(timezone.utc).strftime("%H%M%S%f")[:10]
            # 파일명에 라인 번호 접두사 추가 → spark_batch._line_number_from_name() 호환
            line_num = int(self.line_id.split("_")[1])

            if src_a.exists():
                dst_a = line_dir / f"{line_num}_battery_{bid}_{ts}_laser_a.csv"
                shutil.copy2(src_a, dst_a)

            if src_b.exists():
                dst_b = line_dir / f"{line_num}_battery_{bid}_{ts}_laser_b.csv"
                shutil.copy2(src_b, dst_b)

            self.sent += 1
            print(f"  [생산라인:{self.line_id}] 용접 완료 → battery_{bid} 파일 2개 생성 "
                  f"(누적 {self.sent}개)", flush=True)

            # 다음 용접까지 대기 (실제로는 10초, 시뮬레이션은 interval 설정)
            self.stop_event.wait(timeout=self.interval_sec)

        print(f"  [생산라인:{self.line_id}] 오늘 생산 완료 (총 {self.sent}개)", flush=True)


# ── FileWatcherProducer : Kafka Producer 역할 ────────────────────────────────
#    실제 환경 : watched/ 폴더에 새 파일 감지 → Kafka로 전송
#    시뮬레이션 : 새 파일 감지 → Python queue에 전달

class FileWatcherProducer(threading.Thread):
    def __init__(
        self,
        line_id: str,
        queue_laser_a: queue.Queue,
        queue_laser_b: queue.Queue,
        stop_event: threading.Event,
        poll_interval: float = 1.0,
    ):
        super().__init__(name=f"FileWatcher-{line_id}", daemon=True)
        self.line_id = line_id
        self.queue_laser_a = queue_laser_a
        self.queue_laser_b = queue_laser_b
        self.stop_event = stop_event
        self.poll_interval = poll_interval
        self.seen: set[str] = set()     # 이미 처리한 파일명 (중복 방지)
        self.sent = 0

    def run(self) -> None:
        watch_dir = WATCHED_DIR / self.line_id
        watch_dir.mkdir(parents=True, exist_ok=True)

        print(f"  [FileWatcher:{self.line_id}] 폴더 감시 시작: {watch_dir}", flush=True)

        # 생산라인이 멈추고 5초 후까지 잔여 파일 처리
        idle_deadline = None

        while True:
            if self.stop_event.is_set():
                if idle_deadline is None:
                    idle_deadline = time.monotonic() + 5.0
                if time.monotonic() > idle_deadline:
                    break

            new_files = []
            try:
                for entry in sorted(watch_dir.iterdir(), key=lambda e: e.name):
                    if entry.name in self.seen or not entry.suffix == ".csv":
                        continue
                    # 파일이 완전히 쓰여졌는지 확인 (크기가 0이면 아직 쓰는 중)
                    try:
                        if entry.stat().st_size > 0:
                            new_files.append(entry)
                    except OSError:
                        continue
            except FileNotFoundError:
                pass

            for f in new_files:
                self.seen.add(f.name)

                # 파일명에서 battery_id와 채널 파악
                # 형식: {line_num}_battery_{bid}_{ts}_laser_{ch}.csv
                m = re.search(r"battery_(\d+)", f.stem)
                if not m:
                    continue
                bid = int(m.group(1))

                channel = 1 if "laser_a" in f.name else 0

                msg = Message(
                    file_path=f,
                    battery_id=bid,
                    channel=channel,
                    line_id=self.line_id,
                )

                if channel == 1:
                    self.queue_laser_a.put(msg)
                else:
                    self.queue_laser_b.put(msg)

                self.sent += 1
                print(f"  [Watcher→Kafka:{self.line_id}] battery_{bid} "
                      f"{'laser_a' if channel==1 else 'laser_b'} 전송", flush=True)

            time.sleep(self.poll_interval)

        print(f"  [FileWatcher:{self.line_id}] 종료 (총 {self.sent}개 전송)", flush=True)


# ── Consumer : Spark Streaming 역할 ───────────────────────────────────────────
#    실제 환경 : Kafka 구독 → 도착 즉시 분석 → PostgreSQL 저장
#    시뮬레이션 : queue 구독 → 도착 즉시 분석 → 결과 누적

def summarize_segments(rows: list[dict]) -> dict:
    if not rows:
        return {}
    total = sum(r["sample_count"] for r in rows)
    drift_count = sum(1 for r in rows if r["segment_drift_flag"])
    odd_rows = [r for r in rows if r["parity_group"] == "odd"]
    even_rows = [r for r in rows if r["parity_group"] == "even"]

    def wmean(group):
        w = sum(r["sample_count"] for r in group)
        if w == 0:
            return None
        return sum(r["mean_value"] * r["sample_count"] for r in group
                   if r["mean_value"] is not None) / w

    odd_m, even_m = wmean(odd_rows), wmean(even_rows)
    if odd_m is not None and even_m is not None:
        gap = abs(odd_m - even_m)
        cpd = round(gap / max(abs(odd_m) + abs(even_m), 1e-9), 6)
    else:
        cpd = None

    r0 = rows[0]
    return {
        "battery_id": r0["product_id"],
        "line_id": r0["line_id"],
        "channel": r0["channel"],
        "channel_name": "laser_a" if r0["channel"] == 1 else "laser_b",
        "quality_decision": "drift" if drift_count > 0 else "normal",
        "cpd_score": cpd,
        "drift_segments": f"{drift_count}/{len(rows)}",
        "processed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


class Consumer(threading.Thread):
    def __init__(
        self,
        consumer_id: int,
        channel_name: str,
        msg_queue: queue.Queue,
        run_id: str,
        results: list,
        lock: threading.Lock,
        stats: dict,
        feeder_done_event: threading.Event,
        csv_path: Path,
        processing_delay: float = 0.0,   # 처리 시작 전 인위적 지연 (초)
    ):
        super().__init__(name=f"Consumer-{consumer_id}({channel_name})", daemon=True)
        self.consumer_id = consumer_id
        self.channel_name = channel_name
        self.msg_queue = msg_queue
        self.run_id = run_id
        self.results = results
        self.lock = lock
        self.stats = stats
        self.feeder_done_event = feeder_done_event
        # 채널별 개별 파일 저장 (Sharding)
        base_name = csv_path.stem
        self.csv_path = csv_path.parent / f"{base_name}_{channel_name}.csv"
        
        self.processing_delay = processing_delay
        self.processed = 0

    def run(self) -> None:
        # 생산라인이 멈춘 후 마지막 데이터까지 처리하기 위한 grace period
        GRACE_SEC = 15.0
        idle_since = None

        while True:
            try:
                msg: Message = self.msg_queue.get(timeout=1.0)
                idle_since = None
            except queue.Empty:
                # 생산 완료 후 grace 시간 동안 잔여 메시지 대기
                if self.feeder_done_event.is_set():
                    if idle_since is None:
                        idle_since = time.monotonic()
                    elif time.monotonic() - idle_since > GRACE_SEC:
                        break
                continue

            try:
                _ts = lambda: datetime.now(timezone.utc).isoformat()
                _prog_base = {
                    "consumer_id": self.consumer_id,
                    "channel": self.channel_name,
                    "battery_id": msg.battery_id,
                }

                # 단계 1: 재결합 (처리 지연 시뮬레이션 포함)
                _write_progress({**_prog_base, "stage": "재결합", "ts": _ts()})
                if self.processing_delay > 0:
                    time.sleep(self.processing_delay / 2)
                
                # 단계 2: 용접 구간 데이터 추출
                _write_progress({**_prog_base, "stage": "용접 구간 데이터 추출", "ts": _ts()})
                if self.processing_delay > 0:
                    time.sleep(self.processing_delay / 2)

                segment_rows = build_segment_rows(
                    source_path=msg.file_path,
                    run_id=self.run_id,
                    processed_at=datetime.now(timezone.utc),
                )

                # 단계 3: 드리프트 탐지
                _write_progress({**_prog_base, "stage": "드리프트 탐지", "ts": _ts()})
                summary = summarize_segments(segment_rows)

                if summary:
                    with self.lock:
                        self.results.append(summary)
                        _append_csv(summary, self.csv_path)
                    
                    latency = round(time.monotonic() - msg.arrived_at, 2)
                    print(f"  [Consumer-{self.consumer_id}:{self.channel_name}] "
                          f"{summary['battery_id']} → {summary['quality_decision']} "
                          f"(cpd={summary['cpd_score']}, latency={latency}s)", flush=True)
                    # 최종 단계: 완료
                    _write_progress({
                        **_prog_base,
                        "stage": "완료",
                        "decision": summary.get("quality_decision"),
                        "cpd_score": summary.get("cpd_score"),
                        "ts": _ts(),
                    })

                self.processed += 1
                # 처리 완료 파일 삭제 → watched/ 폴더 lag 지표를 0으로 유지
                try:
                    msg.file_path.unlink(missing_ok=True)
                except OSError:
                    pass
            except Exception as exc:
                print(f"  [Consumer-{self.consumer_id}] ERROR {msg.file_path.name}: {exc}",
                      file=sys.stderr, flush=True)
            finally:
                self.msg_queue.task_done()

        self.stats[f"consumer_{self.consumer_id}_{self.channel_name}"] = self.processed
        print(f"  [Consumer-{self.consumer_id}:{self.channel_name}] 종료 "
              f"(처리 {self.processed}개)", flush=True)


# ── 메인 실험 ─────────────────────────────────────────────────────────────────

def run(
    total_batteries: int,
    line_count: int,
    consumer_count: int,
    interval_sec: float,
    la_delay: float = 0.0,   # laser_a Consumer 처리 지연 (초)
    lb_delay: float = 0.0,   # laser_b Consumer 처리 지연 (초)
) -> None:
    if consumer_count % 2 != 0 or consumer_count < 2:
        raise ValueError("consumer_count는 짝수 2 이상")

    # 사용 가능한 배터리 ID 수집
    available = sorted(
        int(p.stem.split("_battery_")[1].split("_")[0])
        for p in (SOURCE_BASE / "laser_a").glob("20220417_battery_*_laser_a.csv")
    )
    if len(available) < total_batteries:
        raise ValueError(f"요청 {total_batteries}개 > 가용 {len(available)}개")

    selected = available[:total_batteries]

    # 라인별 배터리 분배
    base = total_batteries // line_count
    rem = total_batteries % line_count
    line_batteries: list[tuple[str, list[int]]] = []
    start = 0
    for i in range(line_count):
        size = base + (1 if i < rem else 0)
        lid = f"LINE_{i+1:02d}"
        line_batteries.append((lid, selected[start:start + size]))
        start += size

    # watched/ 폴더 초기화
    if WATCHED_DIR.exists():
        shutil.rmtree(WATCHED_DIR)
    WATCHED_DIR.mkdir(parents=True)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    (ROOT_DIR / "storage" / "logs").mkdir(parents=True, exist_ok=True)

    run_id = str(uuid.uuid4())
    start_wall = datetime.now(timezone.utc)

    # CSV를 실험 시작 시점에 생성 (Consumer가 즉시 append 가능하도록 헤더 선작성)
    ts_str = start_wall.strftime("%Y%m%d_%H%M%S")
    csv_path = METRICS_DIR / f"realtime_{ts_str}_b{total_batteries}_l{line_count}_c{consumer_count}.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as _f:
        csv.DictWriter(_f, fieldnames=CSV_FIELDNAMES).writeheader()

    # 진행 파일 초기화
    PROGRESS_FILE.write_text("", encoding="utf-8")

    print("=" * 62)
    print(f"실무 유사 실험 시작 : {start_wall.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  run_id     : {run_id[:16]}...")
    print(f"  배터리     : {total_batteries}개  |  라인 : {line_count}대  |  컨슈머 : {consumer_count}대")
    print(f"  용접 주기  : {interval_sec}초  (실제: 10초)")
    print(f"  처리 지연  : laser_a={la_delay}초  laser_b={lb_delay}초")
    for lid, bids in line_batteries:
        print(f"  {lid}: battery {bids[0]}~{bids[-1]} ({len(bids)}개)")
    print("=" * 62)
    print("  [구조] DataFeeder → watched/ 폴더 → FileWatcher → queue → Consumer")
    print("  Ctrl+C 로 생산라인 종료")
    print("-" * 62, flush=True)

    queue_laser_a: queue.Queue = queue.Queue()
    queue_laser_b: queue.Queue = queue.Queue()
    results: list[dict] = []
    lock = threading.Lock()
    stats: dict = {}

    stop_event = threading.Event()      # 생산라인 종료 신호
    feeder_done_event = threading.Event()  # 모든 DataFeeder 완료 신호

    # ── DataFeeder 스레드 (생산라인) ──────────────────────────────────────
    feeders = []
    for lid, bids in line_batteries:
        f = DataFeeder(lid, bids, interval_sec, stop_event)
        feeders.append(f)

    # ── FileWatcher 스레드 (라인별 폴더 감시) ─────────────────────────────
    watchers = []
    for lid, _ in line_batteries:
        w = FileWatcherProducer(lid, queue_laser_a, queue_laser_b, stop_event)
        watchers.append(w)

    # ── Consumer 스레드 ───────────────────────────────────────────────────
    consumers = []
    for cid in range(1, consumer_count + 1):
        if cid % 2 == 1:
            chan, q, delay = "laser_a", queue_laser_a, la_delay
        else:
            chan, q, delay = "laser_b", queue_laser_b, lb_delay
        c = Consumer(cid, chan, q, run_id, results, lock, stats, feeder_done_event, csv_path,
                     processing_delay=delay)
        consumers.append(c)

    exp_start = time.monotonic()

    try:
        for t in consumers:
            t.start()
        for t in watchers:
            t.start()
        for t in feeders:
            t.start()

        # DataFeeder 완료 대기
        for t in feeders:
            t.join()

        stop_event.set()         # FileWatcher에게 "생산 완료" 신호 → 5초 후 종료
        feeder_done_event.set()  # Consumer에게 "생산 완료" 신호 → grace period 후 종료
        print(f"\n  [실험] 생산라인 전송 완료 → 컨슈머 드레인 중...", flush=True)

        # Watcher / Consumer 완료 대기
        for t in watchers:
            t.join()
        for t in consumers:
            t.join()

    except KeyboardInterrupt:
        print("\n  [실험] Ctrl+C 감지 → 생산라인 종료 중...", flush=True)
        stop_event.set()
        feeder_done_event.set()
        for t in feeders + watchers + consumers:
            t.join(timeout=10)

    exp_end = time.monotonic()
    total_sec = exp_end - exp_start

    # ── 결과 출력 ─────────────────────────────────────────────────────────
    print("\n" + "=" * 62)
    print("실험 결과")
    print("=" * 62)
    print(f"  총 소요 시간    : {total_sec:.1f}s")
    print(f"  처리 건수       : {len(results)} / 예상 {total_batteries * 2}")
    normal = sum(1 for r in results if r["quality_decision"] == "normal")
    drift  = sum(1 for r in results if r["quality_decision"] == "drift")
    print(f"  normal          : {normal}")
    print(f"  drift           : {drift}")
    print(f"  컨슈머 통계     : {stats}")

    if results:
        results_sorted = sorted(results, key=lambda r: (r["battery_id"], r["channel"]))
        print()
        print(f"  {'battery':<12} {'line':<8} {'ch':<8} {'decision':<8} {'cpd':<10} {'drift_seg'}")
        print("  " + "-" * 58)
        for r in results_sorted:
            print(f"  {r['battery_id']:<12} {r['line_id']:<8} {r['channel_name']:<8} "
                  f"{r['quality_decision']:<8} {str(r['cpd_score']):<10} {r['drift_segments']}")
        print(f"\n  CSV 저장됨 (실시간 기록): {csv_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="실무 유사 실시간 용접 드리프트 탐지 실험")
    p.add_argument("--batteries", type=int,   default=20,  help="총 배터리 수 (기본: 20)")
    p.add_argument("--lines",     type=int,   default=2,   help="생산라인 수 (기본: 2)")
    p.add_argument("--consumers", type=int,   default=2,   help="컨슈머 수 짝수 (기본: 2)")
    p.add_argument("--interval",  type=float, default=10.0,
                   help="DataFeeder 용접 주기 초 (기본: 10.0, 실제 생산라인과 동일)")
    p.add_argument("--la-delay",  type=float, default=0.0,
                   help="laser_a Consumer 처리 지연 초 (기본: 0.0 = 지연 없음)")
    p.add_argument("--lb-delay",  type=float, default=0.0,
                   help="laser_b Consumer 처리 지연 초 (기본: 0.0 = 지연 없음)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(
        total_batteries=args.batteries,
        line_count=args.lines,
        consumer_count=args.consumers,
        interval_sec=args.interval,
        la_delay=args.la_delay,
        lb_delay=args.lb_delay,
    )
