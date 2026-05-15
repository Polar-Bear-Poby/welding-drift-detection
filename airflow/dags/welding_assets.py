from __future__ import annotations

from airflow.sdk import Asset

# Asset events for cross-DAG data-aware scheduling.
PRODUCER_DONE_ASSET = Asset(
    name="welding_producer_done",
    uri="welding://asset/producer_done",
)

BROKER_READY_ASSET = Asset(
    name="welding_broker_ready",
    uri="welding://asset/broker_ready",
)

CONSUMER_PROCESSED_ASSET = Asset(
    name="welding_consumer_processed",
    uri="welding://asset/consumer_processed",
)

DB_QC_COMPLETED_ASSET = Asset(
    name="welding_db_qc_completed",
    uri="welding://asset/db_qc_completed",
)

# Backward-compatibility aliases
INGEST_COMPLETED_ASSET = PRODUCER_DONE_ASSET
SPARK_PROCESS_COMPLETED_ASSET = CONSUMER_PROCESSED_ASSET
