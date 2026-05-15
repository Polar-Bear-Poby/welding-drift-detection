from __future__ import annotations

"""
Spark Structured Streaming for real-time welding drift detection.

This job subscribes to Kafka raw signal chunks, aggregates them into full signals,
performs pattern splitting, and stores the results in PostgreSQL.
"""

import os
import logging
import uuid
import time
import json
import math
import threading
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- Configuration & Logging ---
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("welding.spark_streaming")

def load_env_file(path: str = ".env") -> None:
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

load_env_file()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "").strip()
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "").strip()
LINE_SLOT_INDEX = int(os.getenv("LINE_SLOT_INDEX", "0"))
LINE_SLOT_COUNT = int(os.getenv("LINE_SLOT_COUNT", "1"))
CHANNEL_FILTER_RAW = os.getenv("CHANNEL_FILTER", "").strip()
CHANNEL_FILTER: Optional[int] = None
LINE_FILTER_RAW = os.getenv("LINE_FILTER", "").strip()
LINE_FILTERS = [token.strip() for token in LINE_FILTER_RAW.split(",") if token.strip()]
POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'welding_drift')}?stringtype=unspecified"
POSTGRES_USER = os.getenv("POSTGRES_USER", "welding")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "")
LASER_A_MODEL_NAME = os.getenv("LASER_A_MODEL_NAME", "laser_a_placeholder_model")
LASER_B_MODEL_NAME = os.getenv("LASER_B_MODEL_NAME", "laser_b_placeholder_model")
INFERENCE_DELAY_MS_LASER_A = float(os.getenv("INFERENCE_DELAY_MS_LASER_A", "0"))
INFERENCE_DELAY_MS_LASER_B = float(os.getenv("INFERENCE_DELAY_MS_LASER_B", "0"))
LASER_A_MODEL_VERSION = os.getenv("LASER_A_MODEL_VERSION", "placeholder-v1")
LASER_B_MODEL_VERSION = os.getenv("LASER_B_MODEL_VERSION", "placeholder-v1")
SEGMENT_DRIFT_THRESHOLD_LASER_A = float(os.getenv("SEGMENT_DRIFT_THRESHOLD_LASER_A", "0.62"))
SEGMENT_DRIFT_THRESHOLD_LASER_B = float(os.getenv("SEGMENT_DRIFT_THRESHOLD_LASER_B", "0.58"))
SEGMENT_MIN_SAMPLES = int(os.getenv("SEGMENT_MIN_SAMPLES", "4"))
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "500"))
SPARK_STARTING_OFFSETS = os.getenv("SPARK_STARTING_OFFSETS", "latest")
KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "500"))
KAFKA_MAX_PARTITION_FETCH_BYTES = int(os.getenv("KAFKA_MAX_PARTITION_FETCH_BYTES", "1048576"))
SPARK_TRIGGER_INTERVAL_SEC = int(os.getenv("SPARK_TRIGGER_INTERVAL_SEC", "2"))
ALLOW_RUN_ID_FALLBACK_UUID = os.getenv("ALLOW_RUN_ID_FALLBACK_UUID", "0").strip().lower() in {
    "1", "true", "yes", "on"
}
STRICT_CHANNEL_TOPIC_MATCH = os.getenv("STRICT_CHANNEL_TOPIC_MATCH", "1").strip().lower() in {
    "1", "true", "yes", "on"
}
STAGE_EVENT_RECONCILIATION_ENABLED = os.getenv("STAGE_EVENT_RECONCILIATION_ENABLED", "1").strip().lower() in {
    "1", "true", "yes", "on"
}
STAGE_EVENT_RECON_INTERVAL_SEC = int(os.getenv("STAGE_EVENT_RECON_INTERVAL_SEC", "30"))
STAGE_EVENT_RECON_LOOKBACK_HOURS = int(os.getenv("STAGE_EVENT_RECON_LOOKBACK_HOURS", "24"))
STAGE_EVENT_RECON_LIMIT = int(os.getenv("STAGE_EVENT_RECON_LIMIT", "200"))
LOAD_COMPLETE_GRACE_SEC = int(os.getenv("LOAD_COMPLETE_GRACE_SEC", "180"))
MISSING_HEARTBEAT_GRACE_SEC = int(os.getenv("MISSING_HEARTBEAT_GRACE_SEC", "300"))
LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL = os.getenv(
    "LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL", "1"
).strip().lower() in {"1", "true", "yes", "on"}
LOAD_COMPLETE_MIN_PARTIAL_RATIO = float(os.getenv("LOAD_COMPLETE_MIN_PARTIAL_RATIO", "0.95"))


def parse_channel_filter(raw: str) -> Optional[int]:
    token = raw.strip().lower()
    if not token:
        return None
    if token in ("laser_b", "ch1", "lb", "0"):
        return 0
    if token in ("laser_a", "ch0", "la", "1"):
        return 1
    raise ValueError(
        "CHANNEL_FILTER must be one of: laser_a, laser_b, 0, 1, ch0, ch1, la, lb"
    )


CHANNEL_FILTER = parse_channel_filter(CHANNEL_FILTER_RAW)


def validate_topic_channel_binding(topic_raw: str, channel_filter: Optional[int]) -> None:
    if channel_filter is None:
        return
    topic = topic_raw.strip().lower()
    expected = "laser_a" if channel_filter == 1 else "laser_b"
    if expected in topic:
        return
    message = (
        f"TOPIC_RAW ({topic_raw}) does not match CHANNEL_FILTER "
        f"({expected}). Expected topic containing '{expected}'."
    )
    if STRICT_CHANNEL_TOPIC_MATCH:
        raise ValueError(message)
    logger.warning(message)

# --- JSON Schema for Kafka Messages ---
# Based on producer.py message format
METADATA_SCHEMA = T.StructType([
    T.StructField("source", T.StringType(), True),
    T.StructField("version", T.StringType(), True),
    T.StructField("ingest_run_id", T.StringType(), True),
    T.StructField("file_name", T.StringType(), True),
    T.StructField("original_product_instance_id", T.StringType(), True),
    T.StructField("is_duplicate", T.BooleanType(), True),
    T.StructField("replay_iteration", T.IntegerType(), True),
    T.StructField("chunk_checksum", T.StringType(), True),
])

SIGNAL_MESSAGE_SCHEMA = T.StructType([
    T.StructField("message_id", T.StringType(), False),
    T.StructField("product_instance_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), True),
    T.StructField("line_id", T.StringType(), True),
    T.StructField("lead_num", T.IntegerType(), True),
    T.StructField("channel", T.IntegerType(), True),
    T.StructField("chunk_index", T.IntegerType(), True),
    T.StructField("total_chunks", T.IntegerType(), True),
    T.StructField("start_sample", T.IntegerType(), True),
    T.StructField("end_sample", T.IntegerType(), True),
    # Producer payload uses `samples`; keep `signal` for backward compatibility.
    T.StructField("samples", T.ArrayType(T.FloatType()), True),
    T.StructField("signal", T.ArrayType(T.FloatType()), True),
    T.StructField("event_time", T.StringType(), True),
    T.StructField("metadata", METADATA_SCHEMA, True)
])

# --- Core Analysis Logic (Ported from spark_batch.py) ---
def split_patterns_and_score(signal: List[float], pattern_count: int = 16) -> dict:
    """Split signal and compute a simple cpd proxy score."""
    if not signal or len(signal) < pattern_count:
        # NOTE(temporary): 모델 미적용 기간에는 정상(normal)으로 기록한다.
        # [fix] 'PASS' → 'normal': spark_batch.py와 동일한 값 체계로 통일
        return {"cpd_score": 0.0, "decision": "normal"}

    # Split into 16 segments
    total = len(signal)
    base = total // pattern_count
    remainder = total % pattern_count
    
    odd_means = []
    even_means = []
    
    start = 0
    for i in range(pattern_count):
        size = base + (1 if i < remainder else 0)
        chunk = signal[start:start+size]
        start += size
        
        if not chunk: continue
        mean_val = sum(chunk) / len(chunk)
        
        if (i + 1) % 2 == 1:
            odd_means.append(mean_val)
        else:
            even_means.append(mean_val)
            
    if not odd_means or not even_means:
        # NOTE(temporary): 모델 미적용 기간에는 정상(normal)으로 기록한다.
        # [fix] 'PASS' → 'normal'
        return {"cpd_score": 0.0, "decision": "normal"}
        
    avg_odd = sum(odd_means) / len(odd_means)
    avg_even = sum(even_means) / len(even_means)
    cpd_score = abs(avg_odd - avg_even)
    # NOTE(temporary): 모델 미적용 기간에는 정상(normal)으로 기록한다.
    # [fix] 'PASS' → 'normal': spark_batch.py와 동일한 값 체계로 통일
    decision = "normal"
    
    return {"cpd_score": float(cpd_score), "decision": decision}



def analyze_laser_a(signal: List[float]) -> dict:
    """
    Placeholder route for Laser A model inference.
    TODO: replace proxy with real Laser A drift model inference.
    """
    if INFERENCE_DELAY_MS_LASER_A > 0:
        time.sleep(INFERENCE_DELAY_MS_LASER_A / 1000.0)
    result = split_patterns_and_score(signal)
    result["model_name"] = LASER_A_MODEL_NAME
    return result


def analyze_laser_b(signal: List[float]) -> dict:
    """
    Placeholder route for Laser B model inference.
    TODO: replace proxy with real Laser B drift model inference.
    """
    if INFERENCE_DELAY_MS_LASER_B > 0:
        time.sleep(INFERENCE_DELAY_MS_LASER_B / 1000.0)
    result = split_patterns_and_score(signal)
    result["model_name"] = LASER_B_MODEL_NAME
    return result


def analyze_by_channel(channel: int, signal: List[float]) -> dict:
    if channel == 1:
        return analyze_laser_a(signal)
    if channel == 0:
        return analyze_laser_b(signal)
    # [fix] 'PASS' → 'normal': 알 수 없는 채널도 동일한 값 체계 적용
    return {"cpd_score": 0.0, "decision": "normal", "model_name": "unknown_channel"}


# UDF wrapper for the analysis logic
@F.udf(returnType=T.StructType([
    T.StructField("cpd_score", T.DoubleType(), False),
    T.StructField("decision", T.StringType(), False),
    T.StructField("model_name", T.StringType(), False),
]))
def analyze_signal_udf(channel: int, signal_list: List[float]):
    # signal_list here is a flattened and chunk-index-sorted full signal array.
    return analyze_by_channel(channel, signal_list)

SEGMENT_STRUCT_SCHEMA = T.StructType(
    [
        T.StructField("segment_index", T.IntegerType(), False),
        T.StructField("parity_group", T.StringType(), False),
        T.StructField("parity_order", T.IntegerType(), False),
        T.StructField("sample_count", T.IntegerType(), False),
        T.StructField("mean_value", T.DoubleType(), True),
        T.StructField("std_value", T.DoubleType(), True),
        T.StructField("min_value", T.DoubleType(), True),
        T.StructField("max_value", T.DoubleType(), True),
        T.StructField("model_name", T.StringType(), False),
        T.StructField("model_version", T.StringType(), False),
        T.StructField("inference_score", T.DoubleType(), False),
        T.StructField("segment_drift_flag", T.BooleanType(), False),
        T.StructField("inference_ms", T.IntegerType(), False),
    ]
)


def _split_segments(signal: List[float], pattern_count: int = 16) -> list[list[float]]:
    if not signal:
        return []
    total = len(signal)
    base = total // pattern_count
    remainder = total % pattern_count
    chunks: list[list[float]] = []
    start = 0
    for index in range(pattern_count):
        size = base + (1 if index < remainder else 0)
        end = start + size
        chunks.append(signal[start:end])
        start = end
    return chunks


def _infer_segment_rule(
    channel: int, mean_value: float | None, std_value: float | None, sample_count: int
) -> tuple[str, str, float, bool, int]:
    if channel == 1:
        model_name = LASER_A_MODEL_NAME
        model_version = LASER_A_MODEL_VERSION
        threshold = SEGMENT_DRIFT_THRESHOLD_LASER_A
        inference_ms = int(INFERENCE_DELAY_MS_LASER_A)
    else:
        model_name = LASER_B_MODEL_NAME
        model_version = LASER_B_MODEL_VERSION
        threshold = SEGMENT_DRIFT_THRESHOLD_LASER_B
        inference_ms = int(INFERENCE_DELAY_MS_LASER_B)

    if sample_count < max(SEGMENT_MIN_SAMPLES, 1) or mean_value is None:
        return model_name, model_version, 0.0, False, inference_ms

    std = abs(std_value or 0.0)
    amplitude = abs(mean_value)
    raw_score = amplitude / (amplitude + std + 1e-9)
    inference_score = max(0.0, min(1.0, float(raw_score)))
    drift_flag = inference_score >= threshold
    return model_name, model_version, inference_score, drift_flag, inference_ms


@F.udf(returnType=T.ArrayType(SEGMENT_STRUCT_SCHEMA))
def build_segments_udf(channel: int, signal_list: List[float]):
    if not signal_list:
        return []
    rows = []
    for idx, chunk in enumerate(_split_segments(signal_list, 16), start=1):
        parity_group = "odd" if idx % 2 == 1 else "even"
        parity_order = (idx + 1) // 2 if parity_group == "odd" else idx // 2
        count = int(len(chunk))
        if count == 0:
            mean_value = std_value = min_value = max_value = None
        else:
            mean_value = float(sum(chunk) / count)
            variance = float(sum((sample - mean_value) ** 2 for sample in chunk) / count)
            std_value = float(math.sqrt(variance))
            min_value = float(min(chunk))
            max_value = float(max(chunk))
        (
            model_name,
            model_version,
            inference_score,
            segment_drift_flag,
            inference_ms,
        ) = _infer_segment_rule(
            channel=channel, mean_value=mean_value, std_value=std_value, sample_count=count
        )
        rows.append(
            {
                "segment_index": idx,
                "parity_group": parity_group,
                "parity_order": parity_order,
                "sample_count": count,
                "mean_value": mean_value,
                "std_value": std_value,
                "min_value": min_value,
                "max_value": max_value,
                "model_name": model_name,
                "model_version": model_version,
                "inference_score": inference_score,
                "segment_drift_flag": segment_drift_flag,
                "inference_ms": inference_ms,
            }
        )
    return rows


def _upsert_stage_events_for_completed_runs(run_ids: list[str]) -> None:
    if not run_ids:
        return
    run_ids = [x for x in dict.fromkeys([str(r).strip() for r in run_ids if str(r).strip()])]
    if not run_ids:
        return

    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "welding_drift"),
        user=os.getenv("POSTGRES_USER", "welding"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            for run_id in run_ids:
                expected_products = 0
                expected_channels = 1
                heartbeat_found = False
                producer_experimental = False
                heartbeat_at = None
                cur.execute(
                    """
                    SELECT
                        COALESCE((details->>'target_products_total')::int, 0),
                        GREATEST(COALESCE((details->>'expected_channels')::int, 1), 1),
                        COALESCE((details->>'experimental')::boolean, false),
                        heartbeat_at
                    FROM welding.pipeline_heartbeat
                    WHERE details->>'run_id' = %s
                      AND (
                        (component_name = 'airflow.welding_batch_ingest' AND details->>'status' = 'INGEST_COMPLETED')
                        OR
                        (component_name = 'airflow.welding_producer_asset_dag' AND details->>'status' = 'PRODUCER_DONE')
                      )
                    ORDER BY heartbeat_at DESC
                    LIMIT 1
                    """,
                    (run_id,),
                )
                heartbeat_row = cur.fetchone()
                if heartbeat_row:
                    expected_products = int(heartbeat_row[0] or 0)
                    expected_channels = int(heartbeat_row[1] or 1)
                    producer_experimental = bool(heartbeat_row[2] or False)
                    heartbeat_at = heartbeat_row[3]
                    heartbeat_found = True

                cur.execute(
                    """
                    SELECT MAX(heartbeat_at)
                    FROM welding.pipeline_heartbeat
                    WHERE component_name = 'airflow.welding_producer_asset_dag'
                      AND details->>'status' = 'PRODUCER_DONE'
                    """
                )
                global_latest_producer_done = (cur.fetchone() or [None])[0]

                cur.execute(
                    """
                    SELECT status, started_at, created_at
                    FROM welding.stage_event
                    WHERE run_id = %s::uuid
                      AND stage_name = 'load_complete'
                    LIMIT 1
                    """,
                    (run_id,),
                )
                existing_load_row = cur.fetchone()
                existing_stage_started_at = None
                if existing_load_row:
                    existing_stage_started_at = existing_load_row[1] or existing_load_row[2]

                cur.execute(
                    """
                    SELECT
                        COUNT(DISTINCT product_id),
                        COUNT(*),
                        COUNT(DISTINCT channel)
                    FROM welding.pattern_summary
                    WHERE run_id = %s::uuid
                    """,
                    (run_id,),
                )
                summary_product_count, summary_rows, summary_channel_count = cur.fetchone()
                summary_product_count = int(summary_product_count or 0)
                summary_rows = int(summary_rows or 0)
                summary_channel_count = int(summary_channel_count or 0)

                expected_summary_rows = (
                    expected_products * expected_channels if expected_products > 0 else 0
                )

                cur.execute(
                    "SELECT COUNT(*) FROM welding.pattern_segment WHERE run_id = %s::uuid",
                    (run_id,),
                )
                segment_rows = int((cur.fetchone() or [0])[0] or 0)

                if segment_rows <= 0 or summary_rows <= 0:
                    continue

                progress_ratio = (
                    float(summary_product_count) / float(expected_products)
                    if expected_products > 0
                    else None
                )

                elapsed_from_heartbeat_sec = (
                    max((now - heartbeat_at).total_seconds(), 0.0) if heartbeat_at else None
                )
                stage_anchor = existing_stage_started_at or now
                elapsed_from_stage_sec = max((now - stage_anchor).total_seconds(), 0.0)

                is_load_complete = False
                load_status = "RUNNING"
                load_decision = "waiting"
                error_code = None
                error_message = None

                if heartbeat_found and expected_products > 0:
                    fully_complete = (
                        summary_product_count >= expected_products
                        and summary_rows >= expected_summary_rows
                        and summary_channel_count >= expected_channels
                    )
                    if fully_complete:
                        is_load_complete = True
                        load_status = "SUCCESS"
                        load_decision = "exact_match"
                    else:
                        grace_reached = (
                            elapsed_from_heartbeat_sec is not None
                            and elapsed_from_heartbeat_sec >= max(LOAD_COMPLETE_GRACE_SEC, 1)
                        )
                        ratio = progress_ratio or 0.0
                        if grace_reached:
                            can_partial_success = (
                                producer_experimental
                                and LOAD_COMPLETE_PARTIAL_SUCCESS_EXPERIMENTAL
                                and ratio >= max(min(LOAD_COMPLETE_MIN_PARTIAL_RATIO, 1.0), 0.0)
                                and summary_channel_count >= min(expected_channels, 1)
                            )
                            if can_partial_success:
                                is_load_complete = True
                                load_status = "SUCCESS"
                                load_decision = "partial_success_after_grace"
                                error_code = "PARTIAL_LOAD_ACCEPTED"
                                error_message = (
                                    f"summary_products={summary_product_count}/{expected_products}, "
                                    f"summary_rows={summary_rows}/{expected_summary_rows}, "
                                    f"summary_channels={summary_channel_count}/{expected_channels}, "
                                    f"elapsed_from_heartbeat_sec={elapsed_from_heartbeat_sec:.1f}"
                                )
                            else:
                                load_status = "FAILED"
                                load_decision = "failed_incomplete_after_grace"
                                error_code = "LOAD_INCOMPLETE_AFTER_GRACE"
                                error_message = (
                                    f"summary_products={summary_product_count}/{expected_products}, "
                                    f"summary_rows={summary_rows}/{expected_summary_rows}, "
                                    f"summary_channels={summary_channel_count}/{expected_channels}, "
                                    f"elapsed_from_heartbeat_sec={elapsed_from_heartbeat_sec:.1f}"
                                )
                        else:
                            load_status = "RUNNING"
                            load_decision = "waiting_expected_products"
                elif heartbeat_found and expected_products == 0:
                    # Open-ended run: once we have persisted rows, consider it complete.
                    is_load_complete = True
                    load_status = "SUCCESS"
                    load_decision = "open_ended_with_rows"
                else:
                    # Heartbeat not yet visible. Avoid infinite running by timing out this condition.
                    if elapsed_from_stage_sec >= max(MISSING_HEARTBEAT_GRACE_SEC, 1):
                        # If no producer completion heartbeat exists after this stage started,
                        # producer may still be running. Keep waiting instead of failing early.
                        if (
                            global_latest_producer_done is None
                            or global_latest_producer_done <= stage_anchor
                        ):
                            load_status = "RUNNING"
                            load_decision = "waiting_producer_done_heartbeat"
                        else:
                            load_status = "FAILED"
                            load_decision = "failed_missing_run_heartbeat_after_producer_done"
                            error_code = "HEARTBEAT_NOT_FOUND"
                            error_message = (
                                f"no PRODUCER_DONE heartbeat for run_id={run_id}, "
                                f"elapsed_from_stage_sec={elapsed_from_stage_sec:.1f}, "
                                f"latest_producer_done_at={global_latest_producer_done.isoformat()}"
                            )
                    else:
                        load_status = "RUNNING"
                        load_decision = "waiting_heartbeat"

                if load_status == "RUNNING":
                    logger.info(
                        "run_id=%s progress: summary_products=%s expected_products=%s heartbeat_found=%s "
                        "elapsed_hb=%s elapsed_stage=%.1f decision=%s",
                        run_id,
                        summary_product_count,
                        expected_products,
                        heartbeat_found,
                        (
                            f"{elapsed_from_heartbeat_sec:.1f}"
                            if elapsed_from_heartbeat_sec is not None
                            else "n/a"
                        ),
                        elapsed_from_stage_sec,
                        load_decision,
                    )
                elif load_status == "FAILED":
                    logger.warning(
                        "run_id=%s load_complete FAILED: code=%s msg=%s",
                        run_id,
                        error_code,
                        error_message,
                    )

                common_detail = {
                    "summary_products": summary_product_count,
                    "summary_rows": summary_rows,
                    "summary_channels": summary_channel_count,
                    "segment_rows": segment_rows,
                    "expected_products": expected_products,
                    "expected_channels": expected_channels,
                    "expected_summary_rows": expected_summary_rows,
                    "heartbeat_found": heartbeat_found,
                    "producer_experimental": producer_experimental,
                    "load_decision": load_decision,
                    "elapsed_from_heartbeat_sec": elapsed_from_heartbeat_sec,
                    "elapsed_from_stage_sec": elapsed_from_stage_sec,
                    "progress_ratio": progress_ratio,
                }
                stage_rows = [
                    (
                        run_id,
                        "chunk_complete",
                        "SUCCESS",
                        now,
                        now,
                        json.dumps({**common_detail, "stage": "chunk_complete"}),
                        None,
                        None,
                    ),
                    (
                        run_id,
                        "segmentation_complete",
                        "SUCCESS",
                        now,
                        now,
                        json.dumps({**common_detail, "stage": "segmentation_complete"}),
                        None,
                        None,
                    ),
                    (
                        run_id,
                        "inference_complete",
                        "SUCCESS",
                        now,
                        now,
                        json.dumps({**common_detail, "stage": "inference_complete", "model_mode": "placeholder"}),
                        None,
                        None,
                    ),
                    (
                        run_id,
                        "load_complete",
                        load_status,
                        now,
                        now,
                        json.dumps(
                            {
                                **common_detail,
                                "stage": "load_complete",
                                "error_code": error_code,
                                "error_message": error_message,
                            }
                        ),
                        error_code,
                        error_message,
                    ),
                ]
                rows = [
                    (
                        run_id_,
                        stage_name,
                        status,
                        started_at,
                        ended_at,
                        detail,
                        err_code,
                        err_msg,
                    )
                    for run_id_, stage_name, status, started_at, ended_at, detail, err_code, err_msg in stage_rows
                ]
                execute_values(
                    cur,
                    """
                    INSERT INTO welding.stage_event (
                        run_id, stage_name, status, started_at, ended_at, detail_json, error_code, error_message
                    ) VALUES %s
                    ON CONFLICT (run_id, stage_name) DO UPDATE
                    SET
                        status = EXCLUDED.status,
                        started_at = COALESCE(welding.stage_event.started_at, EXCLUDED.started_at),
                        ended_at = EXCLUDED.ended_at,
                        detail_json = EXCLUDED.detail_json,
                        error_code = EXCLUDED.error_code,
                        error_message = EXCLUDED.error_message
                    """,
                    rows,
                    page_size=100,
                )
        conn.commit()
    finally:
        conn.close()


def _collect_pending_run_ids(limit: int = STAGE_EVENT_RECON_LIMIT) -> list[str]:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "welding_drift"),
        user=os.getenv("POSTGRES_USER", "welding"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH candidate AS (
                    SELECT details->>'run_id' AS run_id, max(heartbeat_at) AS heartbeat_at
                    FROM welding.pipeline_heartbeat
                    WHERE details->>'run_id' IS NOT NULL
                      AND details->>'run_id' <> ''
                      AND (
                        (component_name = 'airflow.welding_producer_asset_dag' AND details->>'status' = 'PRODUCER_DONE')
                        OR
                        (component_name = 'airflow.welding_broker_asset_dag' AND details->>'status' = 'BROKER_READY')
                      )
                      AND heartbeat_at >= NOW() - (%s * INTERVAL '1 hour')
                    GROUP BY details->>'run_id'
                )
                SELECT c.run_id
                FROM candidate c
                LEFT JOIN welding.stage_event se
                  ON se.run_id = c.run_id::uuid
                 AND se.stage_name = 'load_complete'
                 AND se.status IN ('SUCCESS', 'FAILED')
                WHERE se.run_id IS NULL
                ORDER BY c.heartbeat_at DESC
                LIMIT %s
                """,
                (max(STAGE_EVENT_RECON_LOOKBACK_HOURS, 1), max(limit, 1)),
            )
            rows = cur.fetchall() or []
            return [str(row[0]) for row in rows if row and row[0]]
    finally:
        conn.close()


def _stage_event_reconciliation_loop(stop_event: threading.Event) -> None:
    interval = max(STAGE_EVENT_RECON_INTERVAL_SEC, 5)
    while not stop_event.is_set():
        try:
            pending = _collect_pending_run_ids()
            if pending:
                logger.info(
                    "stage_event reconciliation: pending_run_ids=%s sample=%s",
                    len(pending),
                    ",".join(pending[:3]),
                )
                _upsert_stage_events_for_completed_runs(pending)
        except Exception as exc:
            logger.warning("stage_event reconciliation loop error: %s", exc)
        stop_event.wait(interval)

def process_batch(batch_df: DataFrame, batch_id: int):
    """Sink function to write each micro-batch (audit + ready summaries)."""
    if batch_df.isEmpty():
        return

    total_rows = batch_df.count()
    logger.info("Processing micro-batch %s with %s grouped products", batch_id, total_rows)

    # 1) Reassembly audit for all groups (ready + rejected)
    write_reassembly_audit_batch(batch_df, batch_id)

    missing_run_count = batch_df.filter(F.col("missing_ingest_run_id") == F.lit(True)).count()
    if missing_run_count > 0:
        if ALLOW_RUN_ID_FALLBACK_UUID:
            logger.warning(
                "micro-batch %s has %s rows without ingest_run_id. fallback UUID is enabled.",
                batch_id,
                missing_run_count,
            )
        else:
            logger.warning(
                "micro-batch %s has %s rows without ingest_run_id. rows are marked invalid_missing_run_id "
                "and excluded from ready processing.",
                batch_id,
                missing_run_count,
            )

    # 2) Only ready groups move to segment/summarize writes.
    ready_df = (
        batch_df.filter(F.col("reassembly_status") == F.lit("ready"))
        .withColumn("run_id", F.col("resolved_run_id"))
        .withColumn(
            "source_file",
            F.concat(
                F.lit(f"kafka://{TOPIC_RAW}/"),
                F.col("product_instance_id"),
                F.lit("/lead_"),
                F.col("lead_num").cast("string"),
            ),
        )
        .withColumn("line_number", F.regexp_extract(F.col("line_id"), r"LINE_(\\d+)", 1).cast("int"))
        .withColumn(
            "line_number",
            F.when(F.col("line_number").isNull() | (F.col("line_number") <= 0), F.lit(1)).otherwise(
                F.col("line_number")
            ),
        )
        .withColumn("segments", build_segments_udf(F.col("channel"), F.col("full_signal")))
    )
    ready_count = ready_df.count()
    if ready_count == 0:
        logger.info("micro-batch %s has no ready groups after reassembly guard", batch_id)
        return

    segments_to_db = (
        ready_df.select(
            F.col("run_id"),
            F.col("source_file"),
            F.col("channel").cast("short").alias("channel"),
            F.current_timestamp().alias("processed_at"),
            F.to_date(F.col("window.start")).alias("event_date"),
            F.col("line_id"),
            F.col("line_number"),
            F.col("product_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.explode(F.col("segments")).alias("segment"),
        )
        .select(
            "run_id",
            "source_file",
            "channel",
            F.col("segment.segment_index").cast("smallint").alias("segment_index"),
            "processed_at",
            "event_date",
            "line_id",
            "line_number",
            "product_id",
            F.col("segment.parity_group").alias("parity_group"),
            F.col("segment.parity_order").cast("smallint").alias("parity_order"),
            F.col("segment.sample_count").cast("int").alias("sample_count"),
            F.col("segment.mean_value").cast("double").alias("mean_value"),
            F.col("segment.std_value").cast("double").alias("std_value"),
            F.col("segment.min_value").cast("double").alias("min_value"),
            F.col("segment.max_value").cast("double").alias("max_value"),
            F.col("segment.model_name").alias("model_name"),
            F.col("segment.model_version").alias("model_version"),
            F.col("segment.inference_score").cast("double").alias("inference_score"),
            F.col("segment.segment_drift_flag").cast("boolean").alias("segment_drift_flag"),
            F.col("segment.inference_ms").cast("int").alias("inference_ms"),
            "window_start",
            "window_end",
        )
    )

    segments_to_db.select(
        "run_id",
        "source_file",
        "channel",
        "segment_index",
        "processed_at",
        "event_date",
        "line_id",
        "line_number",
        "product_id",
        "parity_group",
        "parity_order",
        "sample_count",
        "mean_value",
        "std_value",
        "min_value",
        "max_value",
        "model_name",
        "model_version",
        "inference_score",
        "segment_drift_flag",
        "inference_ms",
    ).write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "welding.pattern_segment") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()

    agg_df = segments_to_db.groupBy(
        "run_id",
        "source_file",
        "channel",
        "event_date",
        "line_id",
        "line_number",
        "product_id",
        "window_start",
        "window_end",
    ).agg(
        F.count("*").alias("record_count"),
        F.sum("sample_count").alias("total_samples"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("odd"), F.col("mean_value") * F.col("sample_count")).otherwise(
                F.lit(0.0)
            )
        ).alias("odd_weighted_sum"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("odd"), F.col("sample_count")).otherwise(F.lit(0))
        ).alias("odd_sample_sum"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("even"), F.col("mean_value") * F.col("sample_count")).otherwise(
                F.lit(0.0)
            )
        ).alias("even_weighted_sum"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("even"), F.col("sample_count")).otherwise(F.lit(0))
        ).alias("even_sample_sum"),
        F.sum(F.when(F.col("segment_drift_flag"), F.lit(1)).otherwise(F.lit(0))).alias("drift_segment_count"),
    )
    summary_to_db = agg_df.select(
        "run_id",
        "source_file",
        "channel",
        F.current_timestamp().alias("processed_at"),
        "event_date",
        "line_id",
        "line_number",
        "product_id",
        "record_count",
        "total_samples",
        F.when(F.col("odd_sample_sum") > 0, F.col("odd_weighted_sum") / F.col("odd_sample_sum"))
        .otherwise(F.lit(0.0))
        .alias("odd_pattern_mean"),
        F.when(F.col("even_sample_sum") > 0, F.col("even_weighted_sum") / F.col("even_sample_sum"))
        .otherwise(F.lit(0.0))
        .alias("even_pattern_mean"),
        F.abs(
            F.when(F.col("odd_sample_sum") > 0, F.col("odd_weighted_sum") / F.col("odd_sample_sum")).otherwise(
                F.lit(0.0)
            )
            - F.when(
                F.col("even_sample_sum") > 0, F.col("even_weighted_sum") / F.col("even_sample_sum")
            ).otherwise(F.lit(0.0))
        ).alias("odd_even_gap"),
        F.abs(
            F.when(F.col("odd_sample_sum") > 0, F.col("odd_weighted_sum") / F.col("odd_sample_sum")).otherwise(
                F.lit(0.0)
            )
            - F.when(
                F.col("even_sample_sum") > 0, F.col("even_weighted_sum") / F.col("even_sample_sum")
            ).otherwise(F.lit(0.0))
        ).alias("cpd_score"),
        F.col("drift_segment_count").cast("int").alias("drift_segment_count"),
        F.when(F.col("record_count") > 0, F.col("drift_segment_count") / F.col("record_count"))
        .otherwise(F.lit(0.0))
        .alias("drift_segment_ratio"),
        F.when(F.col("drift_segment_count") > 0, F.lit("drift")).otherwise(F.lit("normal")).alias(
            "quality_decision"
        ),
        "window_start",
        "window_end",
    )

    summary_to_db.write \
        .format("jdbc") \
        .option("url", POSTGRES_URL) \
        .option("dbtable", "welding.pattern_summary") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASS) \
        .option("driver", "org.postgresql.Driver") \
        .option("stringtype", "unspecified") \
        .mode("append") \
        .save()

    # Also show in console for monitoring
    summary_to_db.select(
        "window_start",
        "product_id",
        "channel",
        "cpd_score",
        "quality_decision",
    ).show(truncate=False)

    run_ids = [row["run_id"] for row in summary_to_db.select("run_id").distinct().collect() if row["run_id"]]
    _upsert_stage_events_for_completed_runs(run_ids)


def write_reassembly_audit_batch(batch_df: DataFrame, batch_id: int):
    """Write chunk reassembly audit rows for completeness/mixing diagnostics."""
    if batch_df.isEmpty():
        return

    logger.info("Writing reassembly audit batch_id=%s rows=%s", batch_id, batch_df.count())
    audit_to_db = batch_df.select(
        F.current_timestamp().alias("observed_at"),
        F.lit(int(batch_id)).cast("bigint").alias("batch_id"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("product_instance_id"),
        F.col("product_id"),
        F.col("line_id"),
        F.col("lead_num").cast("int"),
        F.col("channel").cast("smallint"),
        F.col("replay_iteration").cast("int"),
        F.col("expected_chunks").cast("int"),
        F.col("received_chunks").cast("int"),
        F.col("unique_chunk_indexes").cast("int"),
        F.col("total_chunks_variants").cast("int"),
        F.col("min_chunk_index").cast("int"),
        F.col("max_chunk_index").cast("int"),
        F.col("expected_samples_from_end").cast("int").alias("expected_samples"),
        F.col("reassembled_samples").cast("int"),
        F.col("reassembly_status"),
        F.col("status_reason"),
    )

    rows = [
        (
            row["observed_at"],
            int(row["batch_id"] or 0),
            row["window_start"],
            row["window_end"],
            row["product_instance_id"],
            row["product_id"],
            row["line_id"],
            int(row["lead_num"] or 0),
            int(row["channel"] or 0),
            int(row["replay_iteration"] or 0),
            int(row["expected_chunks"] or 0),
            int(row["received_chunks"] or 0),
            int(row["unique_chunk_indexes"] or 0),
            int(row["total_chunks_variants"] or 0),
            row["min_chunk_index"],
            row["max_chunk_index"],
            row["expected_samples"],
            row["reassembled_samples"],
            row["reassembly_status"],
            row["status_reason"],
        )
        for row in audit_to_db.collect()
    ]
    if not rows:
        return

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "welding_drift"),
        user=os.getenv("POSTGRES_USER", "welding"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
    )
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO welding.reassembly_audit (
                    observed_at, batch_id, window_start, window_end,
                    product_instance_id, product_id, line_id, lead_num, channel, replay_iteration,
                    expected_chunks, received_chunks, unique_chunk_indexes, total_chunks_variants,
                    min_chunk_index, max_chunk_index, expected_samples, reassembled_samples,
                    reassembly_status, status_reason
                ) VALUES %s
                ON CONFLICT (batch_id, product_instance_id, line_id, lead_num, channel, replay_iteration)
                DO UPDATE SET
                    observed_at = EXCLUDED.observed_at,
                    window_start = EXCLUDED.window_start,
                    window_end = EXCLUDED.window_end,
                    product_id = EXCLUDED.product_id,
                    expected_chunks = EXCLUDED.expected_chunks,
                    received_chunks = EXCLUDED.received_chunks,
                    unique_chunk_indexes = EXCLUDED.unique_chunk_indexes,
                    total_chunks_variants = EXCLUDED.total_chunks_variants,
                    min_chunk_index = EXCLUDED.min_chunk_index,
                    max_chunk_index = EXCLUDED.max_chunk_index,
                    expected_samples = EXCLUDED.expected_samples,
                    reassembled_samples = EXCLUDED.reassembled_samples,
                    reassembly_status = EXCLUDED.reassembly_status,
                    status_reason = EXCLUDED.status_reason
                """,
                rows,
                page_size=500,
            )
        conn.commit()
    except Exception as exc:
        logger.warning("reassembly_audit upsert skipped: %s", exc)
    finally:
        conn.close()

def main():
    if LINE_SLOT_COUNT < 1:
        raise ValueError("LINE_SLOT_COUNT must be >= 1")
    if LINE_SLOT_INDEX < 0 or LINE_SLOT_INDEX >= LINE_SLOT_COUNT:
        raise ValueError("LINE_SLOT_INDEX must satisfy 0 <= index < LINE_SLOT_COUNT")
    if not TOPIC_RAW:
        raise ValueError("TOPIC_RAW must be set (example: welding.raw.laser_a.v1 or welding.raw.laser_b.v1)")
    spark = SparkSession.builder \
        .appName("welding-spark-streaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Subscribing to Kafka topic: {TOPIC_RAW} at {KAFKA_BOOTSTRAP}")
    validate_topic_channel_binding(TOPIC_RAW, CHANNEL_FILTER)
    logger.info(
        "Streaming shard config: slot_index=%s slot_count=%s channel_filter=%s line_filter=%s checkpoint=%s",
        LINE_SLOT_INDEX,
        LINE_SLOT_COUNT,
        CHANNEL_FILTER if CHANNEL_FILTER is not None else "all",
        ",".join(LINE_FILTERS) if LINE_FILTERS else "all",
        SPARK_CHECKPOINT_DIR,
    )
    logger.info(
        "Inference delay config (ms): laser_a=%s laser_b=%s",
        INFERENCE_DELAY_MS_LASER_A,
        INFERENCE_DELAY_MS_LASER_B,
    )
    logger.info(
        "Kafka pull config: startingOffsets=%s maxOffsetsPerTrigger=%s max.poll.records=%s max.partition.fetch.bytes=%s triggerIntervalSec=%s",
        SPARK_STARTING_OFFSETS,
        MAX_OFFSETS_PER_TRIGGER,
        KAFKA_MAX_POLL_RECORDS,
        KAFKA_MAX_PARTITION_FETCH_BYTES,
        SPARK_TRIGGER_INTERVAL_SEC,
    )
    logger.info(
        "Run-id fallback policy: allow_fallback_uuid=%s strict_channel_topic_match=%s",
        ALLOW_RUN_ID_FALLBACK_UUID,
        STRICT_CHANNEL_TOPIC_MATCH,
    )
    logger.info(
        "Stage-event reconciliation: enabled=%s interval_sec=%s lookback_hours=%s limit=%s",
        STAGE_EVENT_RECONCILIATION_ENABLED,
        STAGE_EVENT_RECON_INTERVAL_SEC,
        STAGE_EVENT_RECON_LOOKBACK_HOURS,
        STAGE_EVENT_RECON_LIMIT,
    )
    if KAFKA_GROUP_ID:
        logger.info("Kafka consumer group id: %s", KAFKA_GROUP_ID)
    
    # 1. Read Stream from Kafka
    stream_reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", SPARK_STARTING_OFFSETS) \
        .option("kafka.max.poll.records", str(KAFKA_MAX_POLL_RECORDS)) \
        .option("kafka.max.partition.fetch.bytes", str(KAFKA_MAX_PARTITION_FETCH_BYTES)) \
        .option("maxOffsetsPerTrigger", str(MAX_OFFSETS_PER_TRIGGER))
    if KAFKA_GROUP_ID:
        stream_reader = stream_reader.option("kafka.group.id", KAFKA_GROUP_ID) \
                                     .option("kafka.session.timeout.ms", "10000") \
                                     .option("kafka.commit.interval.ms", "2000")
    raw_stream = stream_reader.load()
    
    # 2. Parse JSON Value
    parsed_stream = raw_stream.select(
        F.from_json(F.col("value").cast("string"), SIGNAL_MESSAGE_SCHEMA).alias("data")
    ).select("data.*")
    
    # 2.1 Cast event_time to Timestamp (Critical for Watermark)
    parsed_stream = parsed_stream.withColumn(
        "event_time", F.to_timestamp("event_time")
    ).withColumn(
        # Producer sends `samples`; fallback to legacy `signal` field if present.
        "signal_values", F.coalesce(F.col("samples"), F.col("signal"))
    ).withColumn(
        "replay_iteration",
        F.coalesce(F.col("metadata.replay_iteration"), F.lit(0)),
    ).filter(
        F.col("signal_values").isNotNull() & (F.size(F.col("signal_values")) > 0)
    ).filter(
        F.col("event_time").isNotNull()
        & F.col("message_id").isNotNull()
        & F.col("product_instance_id").isNotNull()
        & F.col("lead_num").isNotNull()
        & F.col("channel").isNotNull()
        & F.col("line_id").isNotNull()
        & F.col("chunk_index").isNotNull()
        & F.col("total_chunks").isNotNull()
        & (F.col("chunk_index") >= 0)
        & (F.col("total_chunks") > 0)
    )

    if CHANNEL_FILTER is not None:
        parsed_stream = parsed_stream.filter(F.col("channel") == F.lit(CHANNEL_FILTER))
    if LINE_FILTERS:
        parsed_stream = parsed_stream.filter(F.col("line_id").isin(LINE_FILTERS))
    if LINE_SLOT_COUNT > 1:
        parsed_stream = parsed_stream.filter(
            F.pmod(F.xxhash64(F.col("line_id")), F.lit(LINE_SLOT_COUNT))
            == F.lit(LINE_SLOT_INDEX)
        )

    # 3. Dedup + Aggregate Chunks into Full Signal
    deduped_stream = parsed_stream \
        .withWatermark("event_time", "10 minutes") \
        .dropDuplicates(["message_id"])

    aggregated_stream = deduped_stream \
        .groupBy(
            F.window("event_time", "2 minutes"),
            "product_instance_id",
            "product_id",
            "line_id",
            "lead_num",
            "channel",
            "replay_iteration",
        ) \
        .agg(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "chunk_index",
                        "start_sample",
                        "end_sample",
                        "message_id",
                        "total_chunks",
                        "signal_values",
                        F.col("metadata.file_name").alias("file_name"),
                        F.col("metadata.ingest_run_id").alias("ingest_run_id"),
                        F.col("metadata.chunk_checksum").alias("chunk_checksum"),
                    )
                )
            ).alias("sorted_chunks"),
            F.min("total_chunks").alias("min_total_chunks"),
            F.max("total_chunks").alias("max_total_chunks"),
        )

    scored_stream = aggregated_stream.withColumn(
        "expected_chunks",
        F.greatest(F.col("max_total_chunks"), F.lit(0)),
    ).withColumn(
        "total_chunks_variants",
        F.when(
            F.col("min_total_chunks").isNull() | F.col("max_total_chunks").isNull(),
            F.lit(0),
        )
        .when(F.col("min_total_chunks") == F.col("max_total_chunks"), F.lit(1))
        .otherwise(F.lit(2)),
    ).withColumn(
        "received_chunks", F.size(F.col("sorted_chunks"))
    ).withColumn(
        "unique_chunk_indexes",
        F.size(F.array_distinct(F.col("sorted_chunks.chunk_index"))),
    ).withColumn(
        "min_chunk_index", F.array_min(F.col("sorted_chunks.chunk_index"))
    ).withColumn(
        "max_chunk_index", F.array_max(F.col("sorted_chunks.chunk_index"))
    ).withColumn(
        "expected_samples_from_end", F.array_max(F.col("sorted_chunks.end_sample"))
    ).withColumn(
        "full_signal",
        F.flatten(F.col("sorted_chunks.signal_values")),
    ).withColumn(
        "reassembled_samples", F.size(F.col("full_signal"))
    ).withColumn(
        "is_metadata_mixed",
        F.col("min_total_chunks") != F.col("max_total_chunks"),
    ).withColumn(
        "ingest_run_ids",
        F.expr("filter(array_distinct(sorted_chunks.ingest_run_id), x -> x is not null and x <> '')"),
    ).withColumn(
        "missing_ingest_run_id",
        F.size(F.col("ingest_run_ids")) == 0,
    ).withColumn(
        "mixed_ingest_run_id",
        F.size(F.col("ingest_run_ids")) > 1,
    ).withColumn(
        "run_id_missing_unrecoverable",
        F.col("missing_ingest_run_id") & F.lit(not ALLOW_RUN_ID_FALLBACK_UUID),
    ).withColumn(
        "resolved_run_id",
        F.when(
            F.col("missing_ingest_run_id"),
            F.when(F.lit(ALLOW_RUN_ID_FALLBACK_UUID), F.expr("uuid()")).otherwise(F.lit(None)),
        )
        .otherwise(F.element_at(F.col("ingest_run_ids"), 1)),
    ).withColumn(
        "is_chunk_index_incomplete",
        (F.col("expected_chunks") <= 0)
        | (F.col("received_chunks") != F.col("expected_chunks"))
        | (F.col("unique_chunk_indexes") != F.col("expected_chunks"))
        | (F.col("min_chunk_index") != 0)
        | (F.col("max_chunk_index") != (F.col("expected_chunks") - 1)),
    ).withColumn(
        "is_sample_count_mismatch",
        F.col("expected_samples_from_end").isNotNull()
        & (F.col("reassembled_samples") != F.col("expected_samples_from_end")),
    ).withColumn(
        "reassembly_status",
        F.when(F.col("run_id_missing_unrecoverable"), F.lit("invalid_missing_run_id"))
        .when(F.col("is_metadata_mixed") | F.col("mixed_ingest_run_id"), F.lit("mixed_chunk_metadata"))
        .when(F.col("is_chunk_index_incomplete"), F.lit("incomplete_chunks"))
        .when(F.col("is_sample_count_mismatch"), F.lit("sample_count_mismatch"))
        .otherwise(F.lit("ready")),
    ).withColumn(
        "status_reason",
        F.when(
            F.col("run_id_missing_unrecoverable"),
            F.lit("missing ingest_run_id and fallback disabled"),
        )
        .when(F.col("is_metadata_mixed"), F.lit("total_chunks variants > 1"))
        .when(F.col("mixed_ingest_run_id"), F.lit("ingest_run_id variants > 1"))
        .when(
            F.col("is_chunk_index_incomplete"),
            F.concat_ws(
                ", ",
                F.concat(F.lit("expected="), F.col("expected_chunks")),
                F.concat(F.lit("received="), F.col("received_chunks")),
                F.concat(F.lit("unique_idx="), F.col("unique_chunk_indexes")),
                F.concat(F.lit("min_idx="), F.col("min_chunk_index")),
                F.concat(F.lit("max_idx="), F.col("max_chunk_index")),
            ),
        )
        .when(
            F.col("is_sample_count_mismatch"),
            F.concat_ws(
                ", ",
                F.concat(F.lit("expected_samples="), F.col("expected_samples_from_end")),
                F.concat(F.lit("reassembled_samples="), F.col("reassembled_samples")),
            ),
        )
        .otherwise(F.lit("ok")),
    )

    # 4. Sink to PostgreSQL and Console (single query: audit + summary)
    summary_checkpoint = f"{SPARK_CHECKPOINT_DIR.rstrip('/')}/summary"
    recon_stop = threading.Event()
    if STAGE_EVENT_RECONCILIATION_ENABLED:
        threading.Thread(
            target=_stage_event_reconciliation_loop,
            args=(recon_stop,),
            daemon=True,
            name="stage-event-reconciliation",
        ).start()

    query = scored_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", summary_checkpoint) \
        .trigger(processingTime=f"{max(SPARK_TRIGGER_INTERVAL_SEC, 1)} seconds") \
        .start()

    try:
        query.awaitTermination()
    finally:
        recon_stop.set()

if __name__ == "__main__":
    main()


