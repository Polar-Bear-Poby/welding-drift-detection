#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


BATTERY_RE = re.compile(r"battery_(\d+)", re.IGNORECASE)


def extract_battery_id(path: Path) -> str | None:
    matched = BATTERY_RE.search(path.name)
    if not matched:
        return None
    return str(int(matched.group(1)))


def scan_channel_files(channel_dir: Path) -> dict[str, list[str]]:
    by_battery: dict[str, list[str]] = {}
    for csv_path in sorted(channel_dir.glob("**/*.csv")):
        battery_id = extract_battery_id(csv_path)
        if battery_id is None:
            continue
        by_battery.setdefault(battery_id, []).append(str(csv_path))
    return by_battery


def build_index(data_root: Path, date_folder: str) -> dict:
    date_root = data_root / date_folder
    laser_a_dir = date_root / "laser_a"
    laser_b_dir = date_root / "laser_b"

    if not laser_a_dir.exists():
        raise FileNotFoundError(f"laser_a directory not found: {laser_a_dir}")
    if not laser_b_dir.exists():
        raise FileNotFoundError(f"laser_b directory not found: {laser_b_dir}")

    laser_a = scan_channel_files(laser_a_dir)
    laser_b = scan_channel_files(laser_b_dir)

    common_ids = sorted(set(laser_a.keys()) & set(laser_b.keys()), key=int)

    entries = []
    for battery_id in common_ids:
        entries.append(
            {
                "battery_id": battery_id,
                "laser_a_files": laser_a[battery_id],
                "laser_b_files": laser_b[battery_id],
                "laser_a_file_count": len(laser_a[battery_id]),
                "laser_b_file_count": len(laser_b[battery_id]),
            }
        )

    return {
        "date_folder": date_folder,
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "data_root": str(data_root),
        "common_battery_ids": common_ids,
        "common_battery_count": len(common_ids),
        "laser_a_battery_count": len(laser_a),
        "laser_b_battery_count": len(laser_b),
        "entries": entries,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build paired battery index json for date/laser_a and date/laser_b folders."
    )
    parser.add_argument("--data-root", required=True, help="Root data directory (contains YYYYMMDD folder).")
    parser.add_argument("--date-folder", required=True, help="Date folder name, e.g. 20220417")
    parser.add_argument(
        "--output",
        default=None,
        help="Output json path. default: <data-root>/<date-folder>/paired_batteries_index.json",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    data_root = Path(args.data_root).resolve()
    payload = build_index(data_root, args.date_folder)

    output_path = (
        Path(args.output).resolve()
        if args.output
        else (data_root / args.date_folder / "paired_batteries_index.json").resolve()
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {output_path}")
    print(f"common_battery_count: {payload['common_battery_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
