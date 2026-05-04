from __future__ import annotations

import argparse
import re
from pathlib import Path


BATTERY_RE = re.compile(r"battery_(\d+)", re.IGNORECASE)


def detect_channel(path: Path) -> str | None:
    name = path.name.lower()
    if "laser_a" in name or "_ch0" in name:
        return "laser_a"
    if "laser_b" in name or "_ch1" in name:
        return "laser_b"

    parts = [p.lower() for p in path.parts]
    if any(p in ("laser_a", "out", "concat_out_0") for p in parts):
        return "laser_a"
    if any(p in ("laser_b", "reflect", "concat_reflected_1") for p in parts):
        return "laser_b"
    return None


def battery_id(path: Path) -> int | None:
    m = BATTERY_RE.search(path.name)
    if not m:
        return None
    return int(m.group(1))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build stable top-100 paired dataset for session6 experiments."
    )
    parser.add_argument("--source-date-dir", required=True, help="Source date folder path")
    parser.add_argument("--output-date-dir", required=True, help="Output date folder path")
    parser.add_argument("--limit", type=int, default=100, help="Number of batteries to keep")
    args = parser.parse_args()

    src = Path(args.source_date_dir)
    out = Path(args.output_date_dir)
    if not src.exists():
        raise FileNotFoundError(f"source-date-dir not found: {src}")

    out_a = out / "laser_a"
    out_b = out / "laser_b"
    out_a.mkdir(parents=True, exist_ok=True)
    out_b.mkdir(parents=True, exist_ok=True)
    for f in out_a.glob("*.csv"):
        f.unlink()
    for f in out_b.glob("*.csv"):
        f.unlink()

    by_battery: dict[int, dict[str, Path]] = {}
    for p in sorted(src.rglob("*.csv"), key=lambda x: x.name):
        bid = battery_id(p)
        ch = detect_channel(p)
        if bid is None or ch is None:
            continue
        slot = by_battery.setdefault(bid, {})
        if ch not in slot:
            slot[ch] = p

    paired_ids = sorted(
        [bid for bid, channels in by_battery.items() if "laser_a" in channels and "laser_b" in channels]
    )
    selected = paired_ids[: args.limit]
    if len(selected) < args.limit:
        print(f"WARNING: only {len(selected)} paired batteries found (requested={args.limit})")

    for bid in selected:
        item = by_battery[bid]
        dst_a = out_a / f"{out.name}_battery_{bid}_laser_a.csv"
        dst_b = out_b / f"{out.name}_battery_{bid}_laser_b.csv"
        if dst_a.exists():
            dst_a.unlink()
        if dst_b.exists():
            dst_b.unlink()
        dst_a.hardlink_to(item["laser_a"])
        dst_b.hardlink_to(item["laser_b"])

    print(f"source={src}")
    print(f"output={out}")
    print(f"selected_batteries={len(selected)}")
    print(f"first10={selected[:10]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
