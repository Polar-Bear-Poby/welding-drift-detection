#!/usr/bin/env python3
"""Rename concatenated CH0/CH1 files to anonymized battery_<id> format.

Target format:
  YYYYMMDD_battery_<number>_CH0.csv
  YYYYMMDD_battery_<number>_CH1.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path


FILE_RE = re.compile(r"^(?P<date>\d{8})_(?P<battery>.+)_(?P<channel>CH[01])\.csv$")
ANON_BATTERY_RE = re.compile(r"^battery_(\d+)$")


def parse_name(path: Path):
    m = FILE_RE.match(path.name)
    if not m:
        return None
    return m.group("date"), m.group("battery"), m.group("channel")


def collect_files(date: str, roots: list[Path]) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        date_dir = root / date
        if not date_dir.exists():
            continue
        files.extend(sorted(date_dir.glob("*.csv")))
    return files


def build_mapping(files: list[Path], start_id: int) -> dict[str, int]:
    batteries = set()
    for f in files:
        parsed = parse_name(f)
        if not parsed:
            continue
        _date, battery, _channel = parsed
        batteries.add(battery)

    already_numbers = set()
    raw_batteries = []
    for b in sorted(batteries):
        mm = ANON_BATTERY_RE.match(b)
        if mm:
            already_numbers.add(int(mm.group(1)))
        else:
            raw_batteries.append(b)

    mapping: dict[str, int] = {}
    for n in sorted(already_numbers):
        mapping[f"battery_{n}"] = n

    next_id = start_id
    while next_id in already_numbers:
        next_id += 1

    for raw in raw_batteries:
        mapping[raw] = next_id
        next_id += 1
        while next_id in already_numbers:
            next_id += 1

    return mapping


def apply_rename(files: list[Path], mapping: dict[str, int], dry_run: bool) -> tuple[int, int]:
    renamed = 0
    skipped = 0
    for src in sorted(files):
        parsed = parse_name(src)
        if not parsed:
            skipped += 1
            continue
        date, battery, channel = parsed
        if battery not in mapping:
            skipped += 1
            continue
        new_name = f"{date}_battery_{mapping[battery]}_{channel}.csv"
        dst = src.with_name(new_name)

        if src.name == new_name:
            continue
        if dst.exists() and dst.resolve() != src.resolve():
            raise FileExistsError(f"Target file already exists: {dst}")

        print(f"{src.name} -> {new_name}")
        if not dry_run:
            src.rename(dst)
        renamed += 1
    return renamed, skipped


def write_mapping_csv(date: str, mapping: dict[str, int], out_path: Path, dry_run: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"[DRY-RUN] mapping csv: {out_path}")
        return
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "original_battery_token", "anonymized_battery_id"])
        for original in sorted(mapping):
            writer.writerow([date, original, mapping[original]])


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Anonymize concat file names.")
    p.add_argument("--date", default="20220417", help="Target date folder.")
    p.add_argument(
        "--root-ch0",
        default=r"D:\metacode_battery_drfit\data\concat_out_0",
        help="CH0 concat root directory",
    )
    p.add_argument(
        "--root-ch1",
        default=r"D:\metacode_battery_drfit\data\concat_reflected_1",
        help="CH1 concat root directory",
    )
    p.add_argument(
        "--start-id",
        type=int,
        default=1,
        help="First battery id for non-anonymized battery tokens",
    )
    p.add_argument(
        "--mapping-out",
        default=r"D:\metacode_battery_drfit\data\anonymization_maps",
        help="Directory to write mapping csv",
    )
    p.add_argument("--dry-run", action="store_true", help="Print plan only")
    return p


def main() -> None:
    args = build_parser().parse_args()
    roots = [Path(args.root_ch0), Path(args.root_ch1)]
    files = collect_files(args.date, roots)
    if not files:
        print(f"No csv files found for date={args.date}")
        return

    mapping = build_mapping(files, start_id=args.start_id)
    print(f"date={args.date}, files={len(files)}, unique_batteries={len(mapping)}")
    renamed, skipped = apply_rename(files, mapping, dry_run=args.dry_run)
    print(f"renamed={renamed}, skipped={skipped}")

    mapping_csv = Path(args.mapping_out) / f"{args.date}_battery_mapping.csv"
    write_mapping_csv(args.date, mapping, mapping_csv, dry_run=args.dry_run)
    print(f"mapping_file={mapping_csv}")


if __name__ == "__main__":
    main()
