"""
Spark batch preprocessing for welding CSV files.

This job processes multiple CSV files in one run, splits each signal into
16 ordered weld patterns, classifies odd/even pattern groups, computes simple
change-point proxy metrics, and stores outputs to Parquet (and optionally
PostgreSQL).
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import shutil
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

try:
    import psycopg2
    from psycopg2.extras import execute_values
except Exception:  # pragma: no cover - optional runtime dependency
    psycopg2 = None
    execute_values = None

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
except Exception:  # pragma: no cover - optional runtime dependency
    DataFrame = SparkSession = F = T = None


LINE_PREFIX_RE = re.compile(r"^(?P<line>\d+)_")
BATTERY_RE = re.compile(r"battery_(?P<battery_id>\d+)", re.IGNORECASE)
DATE_RE = re.compile(r"(?P<date>20\d{6})")
TRAILING_SUFFIX_RE = re.compile(r"_(?:CH[01]|laser_[ab]|L[AB])$", re.IGNORECASE)


LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt="%H:%M:%S")
logger = logging.getLogger("welding.spark_batch")


def setup_file_logger(output_dir: str) -> None:
    log_dir = Path(output_dir).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "spark_batch.log"
    
    if any(isinstance(h, logging.FileHandler) for h in logger.handlers):
        return

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt="%H:%M:%S"))
    logger.addHandler(file_handler)
    logger.info("File logging enabled: %s", log_file)


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


def default_storage_dir() -> str:
    base = os.getenv("STORAGE_DIR")
    if base:
        return str(Path(base) / "spark_batch")
    return str(Path.cwd() / "storage" / "spark_batch")


@dataclass(frozen=True)
class SourceMeta:
    source_file: str
    file_name: str
    event_date: str
    line_number: int
    line_id: str
    product_id: str
    channel: int


def discover_csv_files(input_dir: str, max_files: int = 0) -> list[Path]:
    files = sorted(Path(input_dir).glob("**/*.csv"))
    if max_files > 0:
        files = files[:max_files]
    return files


def _event_date_from_path(path: Path) -> str:
    for part in path.parts:
        matched = DATE_RE.search(part)
        if matched:
            text = matched.group("date")
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"

    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return mtime.strftime("%Y-%m-%d")


def _line_number_from_name(name: str) -> int:
    matched = LINE_PREFIX_RE.match(name)
    if matched:
        return int(matched.group("line"))
    return 1


def _product_id_from_name(stem: str) -> str:
    matched = BATTERY_RE.search(stem)
    if matched:
        return f"battery_{int(matched.group('battery_id'))}"

    without_line = LINE_PREFIX_RE.sub("", stem)
    return TRAILING_SUFFIX_RE.sub("", without_line)


def _channel_from_path(path: Path) -> int:
    """Infer channel from folder name or filename tokens.

    공개 데이터셋의 익명 채널 토큰으로 채널을 구분한다.
    """
    text = "/".join(part.lower() for part in path.parts)
    if any(token in text for token in ("concat_reflected", "laser_b", "/reflect/", "_lb")):
        return 0
    if any(token in text for token in ("concat_out", "laser_a", "/out/", "_la")):
        return 1
    return 0


def parse_source_metadata(path: Path) -> SourceMeta:
    line_number = _line_number_from_name(path.name)
    product_id = _product_id_from_name(path.stem)
    channel = _channel_from_path(path)
    return SourceMeta(
        source_file=str(path),
        file_name=path.name,
        event_date=_event_date_from_path(path),
        line_number=line_number,
        line_id=f"LINE_{line_number:02d}",
        product_id=product_id,
        channel=channel,
    )


def load_signal(path: Path) -> list[float]:
    values: list[float] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        candidate = line.split(",")[-1].strip()
        try:
            value = float(candidate)
        except ValueError:
            # Skip header/non-numeric rows.
            continue

        if math.isfinite(value):
            values.append(value)

    if not values:
        raise ValueError("no finite numeric samples")
    return values


def split_patterns(signal: list[float], pattern_count: int = 16) -> list[list[float]]:
    """Split signal into equal-size segments (naive 16-equal-split).

    현재 구현: 신호를 단순히 pattern_count 등분한다.
    실제 Change-Point Detection 알고리즘(PELT, CUSUM, Bayesian 등)을
    별도 프로젝트/Colab에서 검증한 뒤 이 함수를 교체할 예정이다.

    TODO(CPD): 실제 CPD 알고리즘 검증 후 아래 균등 분할을
               변화점 기반 분할로 교체할 것.
               교체 시 반환 타입과 pattern_count 의미도 재정의 필요.
    """
    if pattern_count < 1:
        raise ValueError("pattern_count must be >= 1")

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


def build_segment_rows(
    source_path: Path,
    run_id: str,
    processed_at: datetime,
    pattern_count: int = 16,
) -> list[dict]:
    meta = parse_source_metadata(source_path)
    signal = load_signal(source_path)
    rows: list[dict] = []

    for segment_index, chunk in enumerate(split_patterns(signal, pattern_count), start=1):
        parity_group = "odd" if segment_index % 2 == 1 else "even"
        parity_order = (segment_index + 1) // 2 if parity_group == "odd" else segment_index // 2
        count = int(len(chunk))
        if count == 0:
            mean_value = std_value = min_value = max_value = None
        else:
            mean_value = float(sum(chunk) / count)
            variance = float(sum((sample - mean_value) ** 2 for sample in chunk) / count)
            std_value = float(math.sqrt(variance))
            min_value = float(min(chunk))
            max_value = float(max(chunk))

        rows.append(
            {
                "run_id": run_id,
                "processed_at": processed_at,
                "source_file": meta.source_file,
                "file_name": meta.file_name,
                "event_date": meta.event_date,
                "line_number": meta.line_number,
                "line_id": meta.line_id,
                "product_id": meta.product_id,
                "channel": meta.channel,
                "segment_index": segment_index,
                "parity_group": parity_group,
                "parity_order": parity_order,
                "sample_count": count,
                "mean_value": mean_value,
                "std_value": std_value,
                "min_value": min_value,
                "max_value": max_value,
            }
        )

    return rows


def create_spark(master: str, shuffle_partitions: int) -> SparkSession:
    if SparkSession is None:
        raise RuntimeError("pyspark is not installed. install dependencies first.")

    spark = (
        SparkSession.builder.appName("welding-spark-batch")
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_segments_df(spark: SparkSession, rows: list[dict]) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("processed_at", T.TimestampType(), False),
            T.StructField("source_file", T.StringType(), False),
            T.StructField("file_name", T.StringType(), False),
            T.StructField("event_date", T.StringType(), False),
            T.StructField("line_number", T.IntegerType(), False),
            T.StructField("line_id", T.StringType(), False),
            T.StructField("product_id", T.StringType(), False),
            T.StructField("channel", T.IntegerType(), False),
            T.StructField("segment_index", T.IntegerType(), False),
            T.StructField("parity_group", T.StringType(), False),
            T.StructField("parity_order", T.IntegerType(), False),
            T.StructField("sample_count", T.IntegerType(), False),
            T.StructField("mean_value", T.DoubleType(), True),
            T.StructField("std_value", T.DoubleType(), True),
            T.StructField("min_value", T.DoubleType(), True),
            T.StructField("max_value", T.DoubleType(), True),
        ]
    )

    return spark.createDataFrame(rows, schema=schema).withColumn(
        "event_date", F.to_date("event_date", "yyyy-MM-dd")
    )


def build_summary_df(segments_df: DataFrame, cpd_threshold: float) -> DataFrame:
    """Aggregate segment rows into a per-file summary with a drift proxy score.

    cpd_score 현재 구현 (proxy):
        홀수 세그먼트 평균(odd_pattern_mean)과 짝수 세그먼트 평균(even_pattern_mean)의
        상대 차이를 정규화한 값이다.
        → 이것은 진짜 Change-Point Detection (PELT, CUSUM, Bayesian CPD 등)이 아니며,
           실제 알고리즘이 검증되면 아래 TODO 지점에서 교체한다.

    TODO(CPD): Colab/별도 프로젝트에서 실제 CPD 알고리즘 검증 완료 후
               cpd_score 계산 로직(아래 withColumn 블록)을
               실제 변화점 탐지 결과로 교체할 것.
               교체 시 quality_decision 임계값과 레벨(PASS/WARNING/HOLD)도 재정의 필요.
    """
    weighted = segments_df.withColumn("weighted_mean", F.col("mean_value") * F.col("sample_count"))

    grouped = weighted.groupBy(
        "run_id",
        "line_id",
        "line_number",
        "event_date",
        "product_id",
        "source_file",
        "channel",
    ).agg(
        F.max("processed_at").alias("processed_at"),
        F.sum("sample_count").alias("total_samples"),
        F.count(F.lit(1)).alias("record_count"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("odd"), F.col("weighted_mean")).otherwise(F.lit(0.0))
        ).alias("odd_weighted_sum"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("odd"), F.col("sample_count")).otherwise(F.lit(0))
        ).alias("odd_sample_count"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("even"), F.col("weighted_mean")).otherwise(F.lit(0.0))
        ).alias("even_weighted_sum"),
        F.sum(
            F.when(F.col("parity_group") == F.lit("even"), F.col("sample_count")).otherwise(F.lit(0))
        ).alias("even_sample_count"),
    )

    with_mean = (
        grouped.withColumn(
            "odd_pattern_mean",
            F.when(F.col("odd_sample_count") > 0, F.col("odd_weighted_sum") / F.col("odd_sample_count")),
        )
        .withColumn(
            "even_pattern_mean",
            F.when(F.col("even_sample_count") > 0, F.col("even_weighted_sum") / F.col("even_sample_count")),
        )
        .withColumn(
            "odd_even_gap",
            F.abs(F.col("odd_pattern_mean") - F.col("even_pattern_mean")),
        )
        # ── CPD proxy score ──────────────────────────────────────────────────────
        # 현재: |odd_mean - even_mean| / (|odd_mean| + |even_mean|)  [0, 1] 범위
        # 의미: 홀짝 패턴 간 상대 진폭 편차 (Change-Point Detection 아님)
        # TODO(CPD): 아래 withColumn을 실제 CPD 알고리즘 결과로 교체할 것
        # ────────────────────────────────────────────────────────────────────────
        .withColumn(
            "cpd_score",
            F.col("odd_even_gap")
            / F.greatest(
                F.abs(F.col("odd_pattern_mean")) + F.abs(F.col("even_pattern_mean")),
                F.lit(1e-9),
            ),
        )
        .withColumn("window_start", F.date_trunc("hour", F.col("processed_at")))
        .withColumn("window_end", F.expr("window_start + INTERVAL 1 HOUR"))
        # NOTE(temporary): 실제 드리프트 모델이 아직 없으므로
        #                  quality_decision은 임시로 모두 PASS로 고정한다.
        # TODO(CPD): 모델 적용 후 PASS/WARNING/HOLD 등으로 복원.
        .withColumn(
            "quality_decision",
            F.lit("PASS"),
        )
    )

    return with_mean.select(
        "run_id",
        "line_id",
        "line_number",
        "event_date",
        "product_id",
        "source_file",
        "channel",
        "record_count",
        "total_samples",
        "odd_pattern_mean",
        "even_pattern_mean",
        "odd_even_gap",
        "cpd_score",
        "quality_decision",
        "window_start",
        "window_end",
        "processed_at",
    )


def write_parquet(segments_df: DataFrame, summary_df: DataFrame, output_dir: str) -> tuple[str, str]:
    output_root = Path(output_dir)
    segments_path = str(output_root / "segments")
    summary_path = str(output_root / "summary")

    segments_df.write.mode("append").partitionBy("event_date", "line_number").parquet(segments_path)
    summary_df.write.mode("append").partitionBy("event_date", "line_number").parquet(summary_path)
    return segments_path, summary_path


def write_drift_artifacts(
    segments_df: DataFrame,
    summary_df: DataFrame,
    output_dir: str,
    input_dir: str,
) -> tuple[int, int, int, str, str, str]:
    """Persist drift-only artifacts for forensic and model-retraining use.

    Drift candidates are rows whose quality_decision is not PASS.
    We store:
      1) drift summary parquet
      2) drift segment parquet (joined by run_id/source_file/channel)
      3) original raw CSV files for the drift candidates
    """
    output_root = Path(output_dir) / "drift_detected"
    drift_summary_path = str(output_root / "summary")
    drift_segments_path = str(output_root / "segments")
    drift_raw_path = str(output_root / "raw_files")

    drift_summary_df = summary_df.filter(F.col("quality_decision") != F.lit("PASS")).cache()
    drift_summary_count = drift_summary_df.count()
    if drift_summary_count == 0:
        return 0, 0, 0, drift_summary_path, drift_segments_path, drift_raw_path

    drift_summary_df.write.mode("append").partitionBy(
        "event_date", "line_number", "quality_decision"
    ).parquet(drift_summary_path)

    drift_keys_df = drift_summary_df.select("run_id", "source_file", "channel").dropDuplicates()
    drift_segments_df = segments_df.join(
        F.broadcast(drift_keys_df),
        on=["run_id", "source_file", "channel"],
        how="inner",
    ).cache()
    drift_segment_count = drift_segments_df.count()
    drift_segments_df.write.mode("append").partitionBy("event_date", "line_number").parquet(
        drift_segments_path
    )

    input_root = Path(input_dir).resolve()
    raw_output_root = Path(drift_raw_path)
    copied_raw_files = 0
    for row in drift_summary_df.select("source_file").dropDuplicates().collect():
        source_text = row["source_file"]
        if not source_text:
            continue
        source = Path(source_text)
        if not source.exists():
            logger.warning("drift raw source file not found: %s", source_text)
            continue

        source_resolved = source.resolve()
        try:
            rel = source_resolved.relative_to(input_root)
        except Exception:
            rel = Path(source_resolved.name)

        target = raw_output_root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_resolved, target)
        copied_raw_files += 1

    return (
        drift_summary_count,
        drift_segment_count,
        copied_raw_files,
        drift_summary_path,
        drift_segments_path,
        drift_raw_path,
    )


def ensure_postgres_tables(conn) -> None:
    ddl = """
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
    CREATE SCHEMA IF NOT EXISTS welding;

    CREATE TABLE IF NOT EXISTS welding.spark_batch_run (
        run_id UUID PRIMARY KEY,
        status TEXT NOT NULL,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ,
        total_files INTEGER NOT NULL DEFAULT 0,
        total_segment_rows INTEGER NOT NULL DEFAULT 0,
        total_summary_rows INTEGER NOT NULL DEFAULT 0,
        output_dir TEXT NOT NULL,
        details JSONB NOT NULL DEFAULT '{}'::jsonb
    );

    CREATE TABLE IF NOT EXISTS welding.pattern_segment (
        run_id UUID NOT NULL,
        source_file TEXT NOT NULL,
        channel SMALLINT NOT NULL,
        segment_index SMALLINT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL,
        event_date DATE NOT NULL,
        line_id TEXT NOT NULL,
        line_number INTEGER NOT NULL,
        product_id TEXT NOT NULL,
        parity_group TEXT NOT NULL,
        parity_order SMALLINT NOT NULL,
        sample_count INTEGER NOT NULL,
        mean_value DOUBLE PRECISION,
        std_value DOUBLE PRECISION,
        min_value DOUBLE PRECISION,
        max_value DOUBLE PRECISION,
        PRIMARY KEY (run_id, source_file, channel, segment_index)
    );

    CREATE TABLE IF NOT EXISTS welding.pattern_summary (
        run_id UUID NOT NULL,
        source_file TEXT NOT NULL,
        channel SMALLINT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL,
        event_date DATE NOT NULL,
        line_id TEXT NOT NULL,
        line_number INTEGER NOT NULL,
        product_id TEXT NOT NULL,
        record_count INTEGER NOT NULL,
        total_samples INTEGER NOT NULL,
        odd_pattern_mean DOUBLE PRECISION,
        even_pattern_mean DOUBLE PRECISION,
        odd_even_gap DOUBLE PRECISION,
        cpd_score DOUBLE PRECISION,
        quality_decision TEXT NOT NULL,
        window_start TIMESTAMPTZ,
        window_end TIMESTAMPTZ,
        PRIMARY KEY (run_id, source_file, channel)
    );

    CREATE INDEX IF NOT EXISTS idx_pattern_segment_event
        ON welding.pattern_segment (event_date, line_number, product_id);
    CREATE INDEX IF NOT EXISTS idx_pattern_summary_event
        ON welding.pattern_summary (event_date, line_number, quality_decision);
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()


def _iter_tuples(df: DataFrame, columns: list[str]):
    for row in df.select(*columns).toLocalIterator():
        yield tuple(row[column] for column in columns)


def _execute_values_in_batches(conn, sql: str, rows: Iterable[tuple], batch_size: int = 1000) -> int:
    total = 0
    buffer: list[tuple] = []
    with conn.cursor() as cur:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= batch_size:
                execute_values(cur, sql, buffer, page_size=batch_size)
                total += len(buffer)
                buffer.clear()
        if buffer:
            execute_values(cur, sql, buffer, page_size=batch_size)
            total += len(buffer)
    conn.commit()
    return total


def write_postgres(
    summary_df: DataFrame,
    segments_df: DataFrame,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    output_dir: str,
    total_files: int,
    host: str,
    port: int,
    dbname: str,
    user: str,
    password: str,
) -> tuple[int, int]:
    if psycopg2 is None or execute_values is None:
        raise RuntimeError("psycopg2-binary is not installed. install dependencies first.")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    try:
        ensure_postgres_tables(conn)

        segment_cols = [
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
        ]
        segment_sql = """
        INSERT INTO welding.pattern_segment (
            run_id, source_file, channel, segment_index, processed_at, event_date,
            line_id, line_number, product_id, parity_group, parity_order, sample_count,
            mean_value, std_value, min_value, max_value
        ) VALUES %s
        ON CONFLICT (run_id, source_file, channel, segment_index) DO UPDATE
        SET
            processed_at = EXCLUDED.processed_at,
            event_date = EXCLUDED.event_date,
            line_id = EXCLUDED.line_id,
            line_number = EXCLUDED.line_number,
            product_id = EXCLUDED.product_id,
            parity_group = EXCLUDED.parity_group,
            parity_order = EXCLUDED.parity_order,
            sample_count = EXCLUDED.sample_count,
            mean_value = EXCLUDED.mean_value,
            std_value = EXCLUDED.std_value,
            min_value = EXCLUDED.min_value,
            max_value = EXCLUDED.max_value
        """
        segment_count = _execute_values_in_batches(
            conn, segment_sql, _iter_tuples(segments_df, segment_cols), batch_size=2000
        )

        summary_cols = [
            "run_id",
            "source_file",
            "channel",
            "processed_at",
            "event_date",
            "line_id",
            "line_number",
            "product_id",
            "record_count",
            "total_samples",
            "odd_pattern_mean",
            "even_pattern_mean",
            "odd_even_gap",
            "cpd_score",
            "quality_decision",
            "window_start",
            "window_end",
        ]
        summary_sql = """
        INSERT INTO welding.pattern_summary (
            run_id, source_file, channel, processed_at, event_date, line_id, line_number,
            product_id, record_count, total_samples, odd_pattern_mean, even_pattern_mean,
            odd_even_gap, cpd_score, quality_decision, window_start, window_end
        ) VALUES %s
        ON CONFLICT (run_id, source_file, channel) DO UPDATE
        SET
            processed_at = EXCLUDED.processed_at,
            event_date = EXCLUDED.event_date,
            line_id = EXCLUDED.line_id,
            line_number = EXCLUDED.line_number,
            product_id = EXCLUDED.product_id,
            record_count = EXCLUDED.record_count,
            total_samples = EXCLUDED.total_samples,
            odd_pattern_mean = EXCLUDED.odd_pattern_mean,
            even_pattern_mean = EXCLUDED.even_pattern_mean,
            odd_even_gap = EXCLUDED.odd_even_gap,
            cpd_score = EXCLUDED.cpd_score,
            quality_decision = EXCLUDED.quality_decision,
            window_start = EXCLUDED.window_start,
            window_end = EXCLUDED.window_end
        """
        summary_count = _execute_values_in_batches(
            conn, summary_sql, _iter_tuples(summary_df, summary_cols), batch_size=1000
        )

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO welding.spark_batch_run (
                    run_id, status, started_at, finished_at, total_files,
                    total_segment_rows, total_summary_rows, output_dir, details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                ON CONFLICT (run_id) DO UPDATE
                SET
                    status = EXCLUDED.status,
                    finished_at = EXCLUDED.finished_at,
                    total_files = EXCLUDED.total_files,
                    total_segment_rows = EXCLUDED.total_segment_rows,
                    total_summary_rows = EXCLUDED.total_summary_rows,
                    output_dir = EXCLUDED.output_dir
                """,
                (
                    run_id,
                    "SUCCESS",
                    started_at,
                    finished_at,
                    total_files,
                    segment_count,
                    summary_count,
                    output_dir,
                ),
            )
        conn.commit()

        return segment_count, summary_count
    finally:
        conn.close()


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Spark batch preprocessing for welding CSV files",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input-dir", default=os.getenv("DATA_DIR"), required=os.getenv("DATA_DIR") is None)
    parser.add_argument("--output-dir", default=default_storage_dir())
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[*]"))
    parser.add_argument("--shuffle-partitions", type=int, default=10)
    parser.add_argument("--pattern-count", type=int, default=16)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--cpd-threshold", type=float, default=0.35)
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    parser.add_argument("--oldest-date-only", action="store_true", help="Use only CSV files from the oldest date.")

    parser.add_argument("--write-postgres", action="store_true")
    parser.add_argument("--postgres-host", default=os.getenv("POSTGRES_HOST", "localhost"))
    parser.add_argument("--postgres-port", type=int, default=int(os.getenv("POSTGRES_PORT", "15432")))
    parser.add_argument("--postgres-db", default=os.getenv("POSTGRES_DB", "welding_drift"))
    parser.add_argument("--postgres-user", default=os.getenv("POSTGRES_USER", "welding"))
    parser.add_argument("--postgres-password", default=os.getenv("POSTGRES_PASSWORD", "welding_pass"))
    return parser.parse_args(argv)


def run(args: argparse.Namespace) -> int:
    setup_file_logger(args.output_dir)
    logger.info("Starting spark batch preprocessing...")

    started_at = datetime.now(timezone.utc)
    files = discover_csv_files(args.input_dir, args.max_files)
    if not files:
        logger.warning("No CSV files found in %s", args.input_dir)
        return 2

    # --oldest-date-only: parse metadata quickly and filter to oldest date
    if args.oldest_date_only and files:
        parsed_files = []
        for p in files:
            try:
                meta = parse_source_metadata(p)
                parsed_files.append((p, meta.event_date))
            except Exception:
                pass
        if parsed_files:
            oldest = min(parsed_files, key=lambda x: x[1])[1]
            files = [p for p, d in parsed_files if d == oldest]
            logger.info("Filtered to oldest date %s: %s files", oldest, len(files))

    rows: list[dict] = []
    skipped = 0
    for path in files:
        try:
            rows.extend(
                build_segment_rows(
                    source_path=path,
                    run_id=args.run_id,
                    processed_at=started_at,
                    pattern_count=args.pattern_count,
                )
            )
        except Exception as exc:
            skipped += 1
            logger.warning("skip %s: %s", path, exc)

    if not rows:
        logger.error("No valid signals were parsed from input CSV files.")
        return 3

    spark = create_spark(master=args.master, shuffle_partitions=args.shuffle_partitions)
    try:
        segments_df = build_segments_df(spark, rows).cache()
        summary_df = build_summary_df(segments_df, args.cpd_threshold).cache()

        segment_rows = segments_df.count()
        summary_rows = summary_df.count()
        segments_path, summary_path = write_parquet(segments_df, summary_df, args.output_dir)
        (
            drift_summary_rows,
            drift_segment_rows,
            drift_raw_files,
            drift_summary_path,
            drift_segments_path,
            drift_raw_path,
        ) = write_drift_artifacts(
            segments_df=segments_df,
            summary_df=summary_df,
            output_dir=args.output_dir,
            input_dir=args.input_dir,
        )

        logger.info("run_id=%s", args.run_id)
        logger.info("input_files=%s skipped_files=%s", len(files), skipped)
        logger.info("segment_rows=%s summary_rows=%s", segment_rows, summary_rows)
        logger.info("segments_parquet=%s", segments_path)
        logger.info("summary_parquet=%s", summary_path)
        logger.info(
            "drift_summary_rows=%s drift_segment_rows=%s drift_raw_files=%s",
            drift_summary_rows,
            drift_segment_rows,
            drift_raw_files,
        )
        if drift_summary_rows > 0:
            logger.info("drift_summary_parquet=%s", drift_summary_path)
            logger.info("drift_segments_parquet=%s", drift_segments_path)
            logger.info("drift_raw_files_dir=%s", drift_raw_path)

        finished_at = datetime.now(timezone.utc)

        if args.write_postgres:
            seg_written, sum_written = write_postgres(
                summary_df=summary_df,
                segments_df=segments_df,
                run_id=args.run_id,
                started_at=started_at,
                finished_at=finished_at,
                output_dir=args.output_dir,
                total_files=len(files),
                host=args.postgres_host,
                port=args.postgres_port,
                dbname=args.postgres_db,
                user=args.postgres_user,
                password=args.postgres_password,
            )
            logger.info("postgres_written_segments=%s", seg_written)
            logger.info("postgres_written_summary=%s", sum_written)
            finished_at = datetime.now(timezone.utc)

        duration_sec = (finished_at - started_at).total_seconds()
        files_per_sec = len(files) / duration_sec if duration_sec > 0 else 0.0
        segment_rows_per_sec = segment_rows / duration_sec if duration_sec > 0 else 0.0
        summary_rows_per_sec = summary_rows / duration_sec if duration_sec > 0 else 0.0

        logger.info(
            "Batch complete. duration_sec=%.2f files_per_sec=%.2f segment_rows_per_sec=%.2f summary_rows_per_sec=%.2f",
            duration_sec, files_per_sec, segment_rows_per_sec, summary_rows_per_sec
        )
    except Exception as e:
        logger.error("Error during batch processing: %s", e)
        raise
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(run(parse_args()))
