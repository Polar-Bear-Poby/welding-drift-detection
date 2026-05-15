"""
로컬 실험 스크립트 — Docker 없이 uv 가상환경으로 실행

배터리 20개 / 생산라인 2대 / 컨슈머 2대 시뮬레이션

Kafka   → Python queue (in-process)
Spark   → spark_batch.py 분석 함수 직접 호출
Postgres→ 없음 (결과는 CSV + stdout 출력)

사용:
    uv run python scripts/run_local_experiment.py
    uv run python scripts/run_local_experiment.py --batteries 20 --lines 2 --consumers 2
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import queue
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

# spark_batch.py가 프로젝트 루트에 있으므로 루트를 sys.path에 추가
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR))

from spark_batch import (
    build_segment_rows,
    infer_segment_drift,
    load_signal,
    parse_source_metadata,
    split_patterns,
)


def _resolve_data_base() -> Path:
    candidates = [
        Path("D:/metacode_battery_drfit/data_runtime_by_channel/20220417"),
        Path("/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
        Path("/mnt/d/metacode_battery_drfit/data_runtime_by_channel/20220417"),
    ]
    for c in candidates:
        if (c / "laser_a").is_dir():
            return c
    raise FileNotFoundError(f"데이터 디렉터리를 찾을 수 없습니다. 후보: {candidates}")


DATA_BASE = _resolve_data_base()
METRICS_DIR = ROOT_DIR / "storage" / "metrics" / "local_experiment"


# ── 드리프트 판정 (spark_batch build_summary_df 로직 그대로) ─────────────────

def summarize_segments(rows: list[dict]) -> dict:
    """segment rows → pattern_summary 1행 (채널 단위)."""
    if not rows:
        return {}

    total_samples = sum(r["sample_count"] for r in rows)
    record_count = len(rows)
    drift_count = sum(1 for r in rows if r["segment_drift_flag"])

    odd_rows = [r for r in rows if r["parity_group"] == "odd"]
    even_rows = [r for r in rows if r["parity_group"] == "even"]

    def weighted_mean(group):
        total_w = sum(r["sample_count"] for r in group)
        if total_w == 0:
            return None
        return sum(r["mean_value"] * r["sample_count"] for r in group if r["mean_value"] is not None) / total_w

    odd_mean = weighted_mean(odd_rows)
    even_mean = weighted_mean(even_rows)

    if odd_mean is not None and even_mean is not None:
        gap = abs(odd_mean - even_mean)
        denom = max(abs(odd_mean) + abs(even_mean), 1e-9)
        cpd_score = gap / denom
    else:
        gap = cpd_score = None

    quality = "drift" if drift_count > 0 else "normal"

    r0 = rows[0]
    return {
        "run_id": r0["run_id"],
        "product_id": r0["product_id"],
        "line_id": r0["line_id"],
        "channel": r0["channel"],
        "channel_name": "laser_a" if r0["channel"] == 1 else "laser_b",
        "record_count": record_count,
        "total_samples": total_samples,
        "odd_pattern_mean": round(odd_mean, 8) if odd_mean is not None else None,
        "even_pattern_mean": round(even_mean, 8) if even_mean is not None else None,
        "odd_even_gap": round(gap, 8) if gap is not None else None,
        "cpd_score": round(cpd_score, 6) if cpd_score is not None else None,
        "drift_segment_count": drift_count,
        "drift_segment_ratio": round(drift_count / record_count, 4) if record_count else 0,
        "quality_decision": quality,
        "processed_at": r0["processed_at"].isoformat(),
    }


# ── 메시지 구조 ──────────────────────────────────────────────────────────────

class Message:
    """Kafka 메시지를 흉내낸 단순 구조체."""
    __slots__ = ("file_path", "product_id", "line_id", "channel", "line_number", "sent_at")

    def __init__(self, file_path: Path, product_id: str, line_id: str, channel: int, line_number: int):
        self.file_path = file_path
        self.product_id = product_id
        self.line_id = line_id
        self.channel = channel
        self.line_number = line_number
        self.sent_at = time.monotonic()


# ── 프로듀서 ─────────────────────────────────────────────────────────────────

def producer_worker(
    line_number: int,
    battery_ids: list[int],
    queue_laser_a: queue.Queue,
    queue_laser_b: queue.Queue,
    speed: float,
    stats: dict,
) -> None:
    """한 생산라인 = 한 프로듀서 스레드."""
    line_id = f"LINE_{line_number:02d}"
    count = 0
    for bid in battery_ids:
        path_a = DATA_BASE / "laser_a" / f"20220417_battery_{bid}_laser_a.csv"
        path_b = DATA_BASE / "laser_b" / f"20220417_battery_{bid}_laser_b.csv"

        if path_a.exists():
            queue_laser_a.put(Message(path_a, f"battery_{bid}", line_id, 1, line_number))
            count += 1
        if path_b.exists():
            queue_laser_b.put(Message(path_b, f"battery_{bid}", line_id, 0, line_number))
            count += 1

        # speed 배속: 10초 간격을 speed 배 빠르게
        if speed > 0:
            time.sleep(10.0 / speed)

    stats[f"producer_line{line_number}_sent"] = count
    print(f"[Producer-Line{line_number:02d}] done  batteries={len(battery_ids)}  messages={count}")


# ── 컨슈머 ──────────────────────────────────────────────────────────────────

def consumer_worker(
    consumer_id: int,
    channel_name: str,
    channel_id: int,
    msg_queue: queue.Queue,
    run_id: str,
    results: list,
    lock: threading.Lock,
    stats: dict,
    idle_timeout: float = 30.0,
) -> None:
    """한 채널 전담 컨슈머 스레드."""
    processed = 0
    idle_start = None

    while True:
        try:
            msg: Message = msg_queue.get(timeout=1.0)
            idle_start = None
        except queue.Empty:
            if idle_start is None:
                idle_start = time.monotonic()
            elif time.monotonic() - idle_start > idle_timeout:
                break
            continue

        try:
            segment_rows = build_segment_rows(
                source_path=msg.file_path,
                run_id=run_id,
                processed_at=datetime.now(timezone.utc),
            )
            summary = summarize_segments(segment_rows)
            if summary:
                with lock:
                    results.append(summary)
            processed += 1
        except Exception as exc:
            print(f"[Consumer-{consumer_id}] ERROR {msg.file_path.name}: {exc}", file=sys.stderr)
        finally:
            msg_queue.task_done()

    stats[f"consumer_{consumer_id}_{channel_name}_processed"] = processed
    print(f"[Consumer-{consumer_id}({channel_name})] done  processed={processed}")


# ── 메인 실험 ────────────────────────────────────────────────────────────────

def run_experiment(total_batteries: int, line_count: int, consumer_count: int, speed: float) -> None:
    if consumer_count % 2 != 0 or consumer_count < 2:
        raise ValueError("consumer_count는 짝수이고 2 이상이어야 합니다.")
    if line_count < 1:
        raise ValueError("line_count는 1 이상이어야 합니다.")

    # 사용 가능한 배터리 ID 수집 (laser_a 기준)
    available = sorted(
        int(p.stem.split("_battery_")[1].split("_")[0])
        for p in (DATA_BASE / "laser_a").glob("20220417_battery_*_laser_a.csv")
    )
    if len(available) < total_batteries:
        raise ValueError(f"요청 배터리 {total_batteries}개 > 가용 데이터 {len(available)}개")

    selected = available[:total_batteries]

    # 라인별 배터리 분배
    base = total_batteries // line_count
    rem = total_batteries % line_count
    line_batteries: list[list[int]] = []
    start = 0
    for i in range(line_count):
        size = base + (1 if i < rem else 0)
        line_batteries.append(selected[start : start + size])
        start += size

    run_id = str(uuid.uuid4())
    exp_start = time.monotonic()
    exp_start_wall = datetime.now(timezone.utc)

    print("=" * 60)
    print(f"실험 시작: {exp_start_wall.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  run_id={run_id[:12]}...")
    print(f"  batteries={total_batteries}  lines={line_count}  consumers={consumer_count}  speed={speed}x")
    for i, bids in enumerate(line_batteries, 1):
        print(f"  Line{i:02d}: {len(bids)}개 배터리 {bids}")
    print("=" * 60)

    # Kafka 역할 큐
    queue_laser_a: queue.Queue = queue.Queue()
    queue_laser_b: queue.Queue = queue.Queue()

    results: list[dict] = []
    lock = threading.Lock()
    stats: dict = {}

    # 프로듀서 스레드 (라인 수만큼)
    producer_threads = []
    for i, bids in enumerate(line_batteries, 1):
        t = threading.Thread(
            target=producer_worker,
            args=(i, bids, queue_laser_a, queue_laser_b, speed, stats),
            name=f"Producer-Line{i:02d}",
            daemon=True,
        )
        producer_threads.append(t)

    # 컨슈머 idle 타임아웃: 배터리 간격(10s/speed)보다 충분히 길게 설정
    # speed=1이면 배터리 사이 10초 간격 → 최소 20초 대기
    idle_timeout = max(20.0, (10.0 / speed) * 2) if speed > 0 else 15.0

    # 컨슈머 스레드 (홀수=laser_a, 짝수=laser_b)
    consumer_threads = []
    for cid in range(1, consumer_count + 1):
        if cid % 2 == 1:  # 홀수 → laser_a
            chan_name, chan_id, q = "laser_a", 1, queue_laser_a
        else:             # 짝수 → laser_b
            chan_name, chan_id, q = "laser_b", 0, queue_laser_b

        t = threading.Thread(
            target=consumer_worker,
            args=(cid, chan_name, chan_id, q, run_id, results, lock, stats, idle_timeout),
            name=f"Consumer-{cid}({chan_name})",
            daemon=True,
        )
        consumer_threads.append(t)

    # 스레드 시작
    producer_start = time.monotonic()
    for t in consumer_threads:
        t.start()
    for t in producer_threads:
        t.start()

    # 프로듀서 완료 대기
    for t in producer_threads:
        t.join()
    producer_end = time.monotonic()
    producer_duration = producer_end - producer_start
    print(f"\n[실험] 프로듀서 전송 완료: {producer_duration:.1f}s")

    # 컨슈머 완료 대기
    queue_laser_a.join()
    queue_laser_b.join()
    for t in consumer_threads:
        t.join()

    exp_end = time.monotonic()
    total_duration = exp_end - exp_start

    # 결과 집계
    expected_rows = total_batteries * 2  # 배터리 수 × 채널 수
    drift_count = sum(1 for r in results if r.get("quality_decision") == "drift")
    normal_count = sum(1 for r in results if r.get("quality_decision") == "normal")

    print()
    print("=" * 60)
    print("실험 결과 요약")
    print("=" * 60)
    print(f"  전체 소요 시간       : {total_duration:.2f}s")
    print(f"  프로듀서 전송 시간   : {producer_duration:.2f}s")
    print(f"  컨슈머 드레인 시간   : {total_duration - producer_duration:.2f}s")
    print(f"  예상 행 수           : {expected_rows}")
    print(f"  실제 처리 행 수      : {len(results)}")
    print(f"  normal               : {normal_count}")
    print(f"  drift                : {drift_count}")
    print(f"  스레드 통계          : {stats}")
    print()

    # 상세 결과 출력
    results_sorted = sorted(results, key=lambda r: (r["product_id"], r["channel"]))
    print(f"{'product_id':<14} {'line':<9} {'ch':<5} {'channel':<9} {'decision':<8} {'cpd_score':<10} {'drift_seg'}")
    print("-" * 72)
    for r in results_sorted:
        print(
            f"{r['product_id']:<14} {r['line_id']:<9} {r['channel']:<5} "
            f"{r['channel_name']:<9} {r['quality_decision']:<8} "
            f"{str(r['cpd_score']):<10} {r['drift_segment_count']}/{r['record_count']}"
        )

    # CSV 저장
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    ts = exp_start_wall.strftime("%Y%m%d_%H%M%S")
    csv_path = METRICS_DIR / f"local_exp_{ts}_b{total_batteries}_l{line_count}_c{consumer_count}.csv"
    summary_path = METRICS_DIR / f"local_exp_{ts}_b{total_batteries}_l{line_count}_c{consumer_count}_summary.txt"

    if results_sorted:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=results_sorted[0].keys())
            writer.writeheader()
            writer.writerows(results_sorted)

    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(f"run_id={run_id}\n")
        f.write(f"total_batteries={total_batteries}\n")
        f.write(f"line_count={line_count}\n")
        f.write(f"consumer_count={consumer_count}\n")
        f.write(f"speed={speed}\n")
        f.write(f"expected_rows={expected_rows}\n")
        f.write(f"actual_rows={len(results)}\n")
        f.write(f"normal={normal_count}\n")
        f.write(f"drift={drift_count}\n")
        f.write(f"total_duration_sec={total_duration:.2f}\n")
        f.write(f"producer_duration_sec={producer_duration:.2f}\n")
        f.write(f"consumer_drain_sec={total_duration - producer_duration:.2f}\n")

    print()
    print(f"CSV  저장: {csv_path}")
    print(f"요약 저장: {summary_path}")


# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="로컬 용접 드리프트 탐지 실험 (Docker 불필요)")
    p.add_argument("--batteries", type=int, default=20, help="총 배터리 수 (기본: 20)")
    p.add_argument("--lines", type=int, default=2, help="생산라인 수 (기본: 2)")
    p.add_argument("--consumers", type=int, default=2, help="컨슈머 수 짝수 (기본: 2)")
    p.add_argument("--speed", type=float, default=300.0, help="재생 배속 (기본: 300.0, 0=딜레이없음)")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_experiment(
        total_batteries=args.batteries,
        line_count=args.lines,
        consumer_count=args.consumers,
        speed=args.speed,
    )
