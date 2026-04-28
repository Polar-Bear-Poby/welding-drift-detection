"""
Spark Structured Streaming for real-time welding drift detection.

This job subscribes to Kafka raw signal chunks, aggregates them into full signals,
performs pattern splitting, and stores the results in PostgreSQL.
"""

import os
import math
import logging
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List

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
TOPIC_RAW = os.getenv("TOPIC_RAW", "welding.raw.v1")
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "").strip()
LINE_SLOT_INDEX = int(os.getenv("LINE_SLOT_INDEX", "0"))
LINE_SLOT_COUNT = int(os.getenv("LINE_SLOT_COUNT", "1"))
CHANNEL_FILTER_RAW = os.getenv("CHANNEL_FILTER", "").strip()
CHANNEL_FILTER = int(CHANNEL_FILTER_RAW) if CHANNEL_FILTER_RAW else None
LINE_FILTER_RAW = os.getenv("LINE_FILTER", "").strip()
LINE_FILTERS = [token.strip() for token in LINE_FILTER_RAW.split(",") if token.strip()]
POSTGRES_URL = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'welding_drift')}?stringtype=unspecified"
POSTGRES_USER = os.getenv("POSTGRES_USER", "welding")
POSTGRES_PASS = os.getenv("POSTGRES_PASSWORD", "welding_pass")

# --- JSON Schema for Kafka Messages ---
# Based on producer.py message format
METADATA_SCHEMA = T.StructType([
    T.StructField("source", T.StringType(), True),
    T.StructField("version", T.StringType(), True),
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
        # NOTE(temporary): 모델 미적용 기간에는 정상(PASS)으로 기록한다.
        return {"cpd_score": 0.0, "decision": "PASS"}

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
        # NOTE(temporary): 모델 미적용 기간에는 정상(PASS)으로 기록한다.
        return {"cpd_score": 0.0, "decision": "PASS"}
        
    avg_odd = sum(odd_means) / len(odd_means)
    avg_even = sum(even_means) / len(even_means)
    cpd_score = abs(avg_odd - avg_even)
    # NOTE(temporary): 모델 미적용 기간에는 정상(PASS)으로 기록한다.
    decision = "PASS"
    
    return {"cpd_score": float(cpd_score), "decision": decision}

# UDF wrapper for the analysis logic
@F.udf(returnType=T.StructType([
    T.StructField("cpd_score", T.DoubleType(), False),
    T.StructField("decision", T.StringType(), False)
]))
def analyze_signal_udf(signal_list: List[float]):
    # signal_list here is a flattened and chunk-index-sorted full signal array.
    return split_patterns_and_score(signal_list)

@F.udf(returnType=T.StringType())
def generate_uuid_udf():
    return str(uuid.uuid4())

def process_batch(batch_df: DataFrame, batch_id: int):
    """Sink function to write each micro-batch to PostgreSQL."""
    if batch_df.isEmpty():
        return

    logger.info(f"Processing micro-batch {batch_id} with {batch_df.count()} products")
    batch_df.printSchema()
    
    # Write to pattern_summary table
    # We select columns matching welding.pattern_summary schema exactly
    summary_to_db = batch_df.select(
        generate_uuid_udf().alias("run_id"),
        F.lit(f"kafka://{TOPIC_RAW}").alias("source_file"),
        F.col("channel").cast("short"),
        F.current_timestamp().alias("processed_at"),
        F.to_date(F.col("window.start")).alias("event_date"),
        F.col("line_id"),
        F.lit(1).alias("line_number"),
        F.col("product_id"),
        F.lit(16).alias("record_count"),
        F.size(F.col("full_signal")).alias("total_samples"),
        F.lit(0.0).alias("odd_pattern_mean"),
        F.lit(0.0).alias("even_pattern_mean"),
        F.lit(0.0).alias("odd_even_gap"),
        F.col("analysis.cpd_score").alias("cpd_score"),
        F.col("analysis.decision").alias("quality_decision"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end")
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
    batch_df.select("window.start", "product_id", "channel", "analysis.cpd_score", "analysis.decision").show(truncate=False)

def main():
    if LINE_SLOT_COUNT < 1:
        raise ValueError("LINE_SLOT_COUNT must be >= 1")
    if LINE_SLOT_INDEX < 0 or LINE_SLOT_INDEX >= LINE_SLOT_COUNT:
        raise ValueError("LINE_SLOT_INDEX must satisfy 0 <= index < LINE_SLOT_COUNT")
    if CHANNEL_FILTER is not None and CHANNEL_FILTER not in (0, 1):
        raise ValueError("CHANNEL_FILTER must be 0 or 1 when set")

    spark = SparkSession.builder \
        .appName("welding-spark-streaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"Subscribing to Kafka topic: {TOPIC_RAW} at {KAFKA_BOOTSTRAP}")
    logger.info(
        "Streaming shard config: slot_index=%s slot_count=%s channel_filter=%s line_filter=%s checkpoint=%s",
        LINE_SLOT_INDEX,
        LINE_SLOT_COUNT,
        CHANNEL_FILTER if CHANNEL_FILTER is not None else "all",
        ",".join(LINE_FILTERS) if LINE_FILTERS else "all",
        SPARK_CHECKPOINT_DIR,
    )
    if KAFKA_GROUP_ID:
        logger.info("Kafka consumer group id: %s", KAFKA_GROUP_ID)
    
    # 1. Read Stream from Kafka
    stream_reader = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC_RAW) \
        .option("startingOffsets", "earliest")
    if KAFKA_GROUP_ID:
        stream_reader = stream_reader.option("kafka.group.id", KAFKA_GROUP_ID)
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
            "product_instance_id", "product_id", "line_id", "lead_num", "channel"
        ) \
        .agg(
            F.array_sort(
                F.collect_list(
                    F.struct(
                        "chunk_index",
                        "total_chunks",
                        "start_sample",
                        "end_sample",
                        "signal_values",
                        "message_id",
                        F.col("metadata.chunk_checksum").alias("chunk_checksum"),
                    )
                )
            ).alias("sorted_chunks")
        )

    guarded_stream = aggregated_stream.withColumn(
        "expected_chunks",
        F.greatest(F.array_max(F.col("sorted_chunks.total_chunks")), F.lit(0)),
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
    ).filter(
        (F.col("expected_chunks") > 0)
        & (F.col("received_chunks") == F.col("expected_chunks"))
        & (F.col("unique_chunk_indexes") == F.col("expected_chunks"))
        & (F.col("min_chunk_index") == 0)
        & (F.col("max_chunk_index") == (F.col("expected_chunks") - 1))
    )

    # Flatten sorted chunks into a single signal array.
    final_signal_stream = guarded_stream.select(
        "*",
        F.flatten(F.col("sorted_chunks.signal_values")).alias("full_signal"),
    ).withColumn(
        "reassembled_samples", F.size(F.col("full_signal"))
    ).filter(
        F.col("expected_samples_from_end").isNull()
        | (F.col("reassembled_samples") == F.col("expected_samples_from_end"))
    )
    
    # 4. Analyze Signal
    analyzed_stream = final_signal_stream.withColumn(
        "analysis", analyze_signal_udf(F.col("full_signal"))
    )
    
    # 5. Sink to PostgreSQL and Console
    query = analyzed_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("update") \
        .option("checkpointLocation", SPARK_CHECKPOINT_DIR) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
