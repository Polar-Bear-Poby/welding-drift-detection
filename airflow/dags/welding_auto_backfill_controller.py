"""
welding_auto_backfill_controller.py
==================================
Missing-date auto backfill controller.

- 수동 백필 중심 운영은 유지
- 누락 날짜만 자동으로 `welding_batch_backfill` DAG를 트리거
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from datetime import datetime, timedelta

import psycopg2
from airflow.decorators import dag, task

log = logging.getLogger(__name__)

_PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
_PG_PORT = os.getenv("POSTGRES_PORT", "5432")
_PG_DB = os.getenv("POSTGRES_DB", "welding_drift")
_PG_USER = os.getenv("POSTGRES_USER", "welding")
_PG_PASS = os.getenv("POSTGRES_PASSWORD", "")
DB_CONN_STR = (
    f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_DB} "
    f"user={_PG_USER} password={_PG_PASS}"
)

PRODUCER_CONTAINER = os.getenv("PRODUCER_CONTAINER", "welding-producer")
DATA_DIR_CONTAINER = os.getenv("DATA_DIR_CONTAINER", "/data")
MIN_EXPECTED_ROWS = int(os.getenv("AUTO_BACKFILL_MIN_EXPECTED_ROWS", "6"))
LOOKBACK_DAYS = int(os.getenv("AUTO_BACKFILL_LOOKBACK_DAYS", "30"))
MAX_TRIGGER_DATES = int(os.getenv("AUTO_BACKFILL_MAX_TRIGGER_DATES", "3"))
TRIGGER_COOLDOWN_HOURS = int(os.getenv("AUTO_BACKFILL_TRIGGER_COOLDOWN_HOURS", "24"))

BACKFILL_LINE_COUNT = int(os.getenv("AUTO_BACKFILL_LINE_COUNT", "3"))
BACKFILL_LINE_SEEDS = os.getenv("AUTO_BACKFILL_LINE_SEEDS", "42,73,128")
BACKFILL_REPLAY_SPEED = float(os.getenv("AUTO_BACKFILL_REPLAY_SPEED", "100"))


def _list_candidate_dates() -> list[str]:
    """Read available YYYYMMDD folders in producer /data."""
    discover_code = (
        "import pathlib, re\n"
        f"root = pathlib.Path('{DATA_DIR_CONTAINER}')\n"
        "pat = re.compile(r'^20\\d{6}$')\n"
        "out = []\n"
        "if root.exists():\n"
        "  for p in root.iterdir():\n"
        "    if not p.is_dir() or not pat.match(p.name):\n"
        "      continue\n"
        "    has_csv = any(x.suffix.lower()=='.csv' for x in p.rglob('*.csv'))\n"
        "    if has_csv:\n"
        "      out.append(p.name)\n"
        "print('\\n'.join(sorted(out)), end='')\n"
    )
    cmd = (
        f"docker start {PRODUCER_CONTAINER} >/dev/null 2>&1 || true && "
        f"docker exec {PRODUCER_CONTAINER} python -c \"{discover_code}\""
    )
    proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError(f"Failed to list date folders: {proc.stderr.strip()}")
    return [line.strip() for line in (proc.stdout or "").splitlines() if line.strip()]


@dag(
    dag_id="welding_auto_backfill_controller",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["welding", "airflow3", "backfill", "auto-controller"],
    default_args={"owner": "welding-team", "retries": 1, "retry_delay": timedelta(minutes=10)},
)
def welding_auto_backfill_controller_dag():

    @task()
    def find_missing_dates() -> list[str]:
        date_folders = _list_candidate_dates()
        if not date_folders:
            return []

        cutoff = datetime.now().date() - timedelta(days=LOOKBACK_DAYS)
        scoped = []
        for raw in date_folders:
            try:
                iso = f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
                d = datetime.strptime(iso, "%Y-%m-%d").date()
                if d >= cutoff:
                    scoped.append((raw, iso))
            except Exception:
                continue
        scoped.sort(key=lambda x: x[0])

        missing: list[str] = []
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                for raw, iso in scoped:
                    cur.execute(
                        "SELECT COUNT(*) FROM welding.pattern_summary WHERE event_date = %s",
                        (iso,),
                    )
                    row_count = cur.fetchone()[0] or 0
                    if row_count >= MIN_EXPECTED_ROWS:
                        continue

                    cur.execute(
                        """
                        SELECT COUNT(*)
                        FROM welding.pipeline_heartbeat
                        WHERE component_name = 'airflow.auto_backfill_controller'
                          AND details->>'status' = 'BACKFILL_TRIGGERED'
                          AND details->>'target_date' = %s
                          AND heartbeat_at >= NOW() - (%s || ' hours')::interval
                        """,
                        (iso, TRIGGER_COOLDOWN_HOURS),
                    )
                    recently_triggered = (cur.fetchone()[0] or 0) > 0
                    if recently_triggered:
                        continue

                    missing.append(raw)
                    if len(missing) >= MAX_TRIGGER_DATES:
                        break

        log.info("Auto backfill missing dates: %s", missing)
        return missing

    @task.short_circuit()
    def has_missing_dates(dates: list[str]) -> bool:
        return len(dates) > 0

    @task()
    def trigger_backfill(target_date_raw: str) -> str:
        conf = {
            "target_date": target_date_raw,
            "line_count": BACKFILL_LINE_COUNT,
            "line_seeds": BACKFILL_LINE_SEEDS,
            "replay_speed": BACKFILL_REPLAY_SPEED,
            "force_overwrite": False,
        }
        cmd = [
            "airflow",
            "dags",
            "trigger",
            "welding_batch_backfill",
            "--conf",
            json.dumps(conf),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if proc.returncode != 0:
            raise RuntimeError(
                f"Failed to trigger backfill for {target_date_raw}: {proc.stderr.strip()}"
            )

        target_iso = f"{target_date_raw[0:4]}-{target_date_raw[4:6]}-{target_date_raw[6:8]}"
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    (
                        "airflow.auto_backfill_controller",
                        json.dumps(
                            {
                                "status": "BACKFILL_TRIGGERED",
                                "target_date": target_iso,
                                "conf": conf,
                            }
                        ),
                    ),
                )
        log.info("Triggered welding_batch_backfill for date=%s", target_date_raw)
        return target_date_raw

    @task()
    def write_summary(triggered_dates: list[str]):
        with psycopg2.connect(DB_CONN_STR) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO welding.pipeline_heartbeat (component_name, details)
                    VALUES (%s, %s::jsonb)
                    """,
                    (
                        "airflow.auto_backfill_controller",
                        json.dumps(
                            {
                                "status": "AUTO_BACKFILL_SUMMARY",
                                "triggered_count": len(triggered_dates),
                                "triggered_dates": triggered_dates,
                            }
                        ),
                    ),
                )

    missing_dates = find_missing_dates()
    gate = has_missing_dates(missing_dates)
    triggered = trigger_backfill.expand(target_date_raw=missing_dates)
    summary = write_summary(triggered)
    gate >> triggered >> summary


welding_auto_backfill_controller = welding_auto_backfill_controller_dag()
