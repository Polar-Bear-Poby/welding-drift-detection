from __future__ import annotations

from datetime import date
from typing import Any

import psycopg2


def _channel_name(channel_num: int) -> str:
    return "laser_a" if int(channel_num) == 1 else "laser_b"


def fetch_latest_quality(
    conn: psycopg2.extensions.connection, limit: int = 50
) -> list[dict[str, Any]]:
    sql = """
    SELECT
      run_id::text AS run_id,
      product_id,
      line_id,
      channel,
      quality_decision,
      cpd_score,
      odd_even_gap,
      record_count,
      total_samples,
      event_date,
      processed_at
    FROM welding.pattern_summary
    ORDER BY processed_at DESC
    LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
    for row in rows:
        row["channel"] = _channel_name(row["channel"])
    return rows


def fetch_quality_history(
    conn: psycopg2.extensions.connection,
    start_date: date,
    end_date: date,
    line_id: str | None,
    channel: str | None,
    limit: int = 3000,
) -> list[dict[str, Any]]:
    clauses = ["event_date BETWEEN %s AND %s"]
    params: list[Any] = [start_date, end_date]

    if line_id:
        clauses.append("line_id = %s")
        params.append(line_id)

    if channel:
        channel_num = 1 if channel == "laser_a" else 0
        clauses.append("channel = %s")
        params.append(channel_num)

    params.append(limit)
    sql = f"""
    SELECT
      run_id::text AS run_id,
      product_id,
      line_id,
      channel,
      quality_decision,
      cpd_score,
      odd_even_gap,
      record_count,
      total_samples,
      event_date,
      processed_at
    FROM welding.pattern_summary
    WHERE {" AND ".join(clauses)}
    ORDER BY processed_at DESC
    LIMIT %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    for row in rows:
        row["channel"] = _channel_name(row["channel"])
    return rows


def fetch_battery_quality(
    conn: psycopg2.extensions.connection, product_id: str
) -> list[dict[str, Any]]:
    sql = """
    SELECT
      run_id::text AS run_id,
      product_id,
      line_id,
      channel,
      quality_decision,
      cpd_score,
      odd_even_gap,
      record_count,
      total_samples,
      event_date,
      processed_at
    FROM welding.pattern_summary
    WHERE product_id = %s
    ORDER BY processed_at DESC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (product_id,))
        rows = cur.fetchall()
    for row in rows:
        row["channel"] = _channel_name(row["channel"])
    return rows


def fetch_run_summary(
    conn: psycopg2.extensions.connection, run_id: str
) -> dict[str, Any] | None:
    sql = """
    SELECT
      run_id::text AS run_id,
      status,
      started_at,
      finished_at,
      total_files,
      total_segment_rows,
      total_summary_rows,
      output_dir,
      details
    FROM welding.spark_batch_run
    WHERE run_id::text = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        row = cur.fetchone()
    if not row:
        return None

    details = row.get("details") or {}
    row["details"] = details
    row["line_count"] = details.get("line_count")
    row["producer_count"] = details.get("producer_count")
    return row


def fetch_pattern_segments(
    conn: psycopg2.extensions.connection, product_id: str
) -> list[dict[str, Any]]:
    """특정 배터리의 16균등 분할 세그먼트 상세 데이터를 조회합니다."""
    sql = """
    SELECT
        product_id,
        channel,
        segment_index,
        sample_count,
        mean_value,
        std_value,
        inference_score,
        segment_drift_flag,
        processed_at
    FROM welding.pattern_segment
    WHERE product_id = %s
    ORDER BY channel, segment_index
    """
    with conn.cursor() as cur:
        cur.execute(sql, (product_id,))
        rows = cur.fetchall()
    for row in rows:
        row["channel"] = _channel_name(row["channel"])
    return rows

