#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


def detect_channel(name: str) -> str | None:
    low = name.lower()
    if "laser_a" in low:
        return "laser_a"
    if "laser_b" in low:
        return "laser_b"
    if "_ch0" in low:
        return "laser_a"
    if "_ch1" in low:
        return "laser_b"
    return None


def main() -> int:
    p = argparse.ArgumentParser(
        description="Build date/laser_a|laser_b layout from a flat date directory."
    )
    p.add_argument("--source-root", required=True, help="e.g. D:/.../data_runtime_flat")
    p.add_argument("--target-root", required=True, help="e.g. D:/.../data_runtime_by_channel")
    p.add_argument("--date", default="20220417", help="target date folder")
    args = p.parse_args()

    source_date = Path(args.source_root) / args.date
    target_date = Path(args.target_root) / args.date
    target_a = target_date / "laser_a"
    target_b = target_date / "laser_b"

    if not source_date.exists():
        raise FileNotFoundError(f"source date folder not found: {source_date}")

    target_a.mkdir(parents=True, exist_ok=True)
    target_b.mkdir(parents=True, exist_ok=True)

    for f in target_a.glob("*.csv"):
        f.unlink()
    for f in target_b.glob("*.csv"):
        f.unlink()

    count_a = 0
    count_b = 0
    skipped = 0
    for src in sorted(source_date.rglob("*.csv"), key=lambda x: x.name):
        ch = detect_channel(src.name)
        if ch is None:
            skipped += 1
            continue
        dst = (target_a if ch == "laser_a" else target_b) / src.name
        if dst.exists():
            dst.unlink()
        dst.hardlink_to(src)
        if ch == "laser_a":
            count_a += 1
        else:
            count_b += 1

    print(f"source={source_date}")
    print(f"target={target_date}")
    print(f"laser_a_files={count_a}")
    print(f"laser_b_files={count_b}")
    print(f"skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

