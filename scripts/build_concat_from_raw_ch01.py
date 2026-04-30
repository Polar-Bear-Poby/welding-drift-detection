#!/usr/bin/env python3
"""Build concatenated per-battery CSV files from raw CH0/CH1 lead files.

Input filename format (example):
  20220417_104528_1_LGP-POL17.04.22F7VDH034_15_CH0.csv

Parsed as:
  date_process_seq_battery_id_lead_channel.csv

For each channel:
  - group files by battery_id
  - sort by lead number ascending
  - concatenate rows in lead order
  - write one output file per battery

Default output paths:
  CH0 -> D:\\metacode_battery_drfit\\data\\concat_out_0\\<date>
  CH1 -> D:\\metacode_battery_drfit\\data\\concat_reflected_1\\<date>
"""

from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List


NAME_RE = re.compile(
    r"^(?P<date>\d{8})_(?P<process>\d{6})_(?P<seq>[^_]+)_(?P<battery>.+)_(?P<lead>\d{2})_(?P<channel>CH[01])\.csv$"
)


@dataclass(frozen=True)
class LeadFile:
    path: Path
    date: str
    process: str
    seq: str
    battery: str
    lead: int
    channel: str


def parse_file(path: Path) -> LeadFile | None:
    match = NAME_RE.match(path.name)
    if not match:
        return None
    parts = match.groupdict()
    return LeadFile(
        path=path,
        date=parts["date"],
        process=parts["process"],
        seq=parts["seq"],
        battery=parts["battery"],
        lead=int(parts["lead"]),
        channel=parts["channel"],
    )


def collect_files(src_dir: Path, date: str, channel: str) -> List[LeadFile]:
    records: List[LeadFile] = []
    for file_path in sorted(src_dir.rglob("*.csv")):
        parsed = parse_file(file_path)
        if parsed is None:
            continue
        if parsed.date != date or parsed.channel != channel:
            continue
        records.append(parsed)
    return records


def group_by_battery(records: Iterable[LeadFile]) -> Dict[str, List[LeadFile]]:
    grouped: Dict[str, List[LeadFile]] = {}
    for record in records:
        grouped.setdefault(record.battery, []).append(record)
    for battery, items in grouped.items():
        items.sort(key=lambda x: (x.lead, x.process, x.seq, x.path.name))
        grouped[battery] = items
    return grouped


def concat_group_to_file(items: List[LeadFile], out_path: Path) -> int:
    line_count = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="\n") as out_f:
        for lead_file in items:
            with lead_file.path.open("r", encoding="utf-8", errors="ignore") as in_f:
                for raw_line in in_f:
                    line = raw_line.strip()
                    if not line:
                        continue
                    out_f.write(line + "\n")
                    line_count += 1
    return line_count


def run_channel(
    src_dir: Path,
    out_root: Path,
    date: str,
    channel: str,
    dry_run: bool,
) -> None:
    records = collect_files(src_dir, date=date, channel=channel)
    grouped = group_by_battery(records)
    out_dir = out_root / date
    print(f"[{channel}] source={src_dir}")
    print(f"[{channel}] parsed_files={len(records)}, batteries={len(grouped)}")

    for battery_id, items in sorted(grouped.items()):
        out_name = f"{date}_{battery_id}_{channel}.csv"
        out_path = out_dir / out_name
        leads = [x.lead for x in items]
        if dry_run:
            print(
                f"[DRY-RUN][{channel}] {out_path} <- {len(items)} files, leads={leads[:5]}{'...' if len(leads) > 5 else ''}"
            )
            continue
        lines = concat_group_to_file(items, out_path)
        print(
            f"[{channel}] wrote: {out_path} (input_files={len(items)}, lines={lines}, lead_min={min(leads)}, lead_max={max(leads)})"
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Concatenate raw CH0/CH1 lead files by battery and lead order."
    )
    parser.add_argument(
        "--date",
        default="20220417",
        help="Target date folder token in file name (default: 20220417).",
    )
    parser.add_argument(
        "--src-ch0",
        default=r"D:\metacode_battery_drfit\data\0",
        help="Source directory for CH0 raw files.",
    )
    parser.add_argument(
        "--src-ch1",
        default=r"D:\metacode_battery_drfit\data\1",
        help="Source directory for CH1 raw files.",
    )
    parser.add_argument(
        "--out-ch0",
        default=r"D:\metacode_battery_drfit\data\concat_out_0",
        help="Output root directory for CH0 merged files.",
    )
    parser.add_argument(
        "--out-ch1",
        default=r"D:\metacode_battery_drfit\data\concat_reflected_1",
        help="Output root directory for CH1 merged files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be written without creating files.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    run_channel(
        src_dir=Path(args.src_ch0),
        out_root=Path(args.out_ch0),
        date=args.date,
        channel="CH0",
        dry_run=args.dry_run,
    )
    run_channel(
        src_dir=Path(args.src_ch1),
        out_root=Path(args.out_ch1),
        date=args.date,
        channel="CH1",
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
