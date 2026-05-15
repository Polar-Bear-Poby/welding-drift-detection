"""
welding_realtime_assets.py
===========================
new_src/ 아키텍처 전용 Asset 정의.
FileWatcher 기반 실시간 파이프라인 (Docker/Kafka/PostgreSQL 없음).

기존 welding_assets.py와 독립적으로 관리.
Cross-DAG 데이터 인식 스케줄링에 사용.

파이프라인 Asset 체인 (기존과 동일한 개념적 흐름):
  REALTIME_PRODUCER_DONE_ASSET
    → REALTIME_BROKER_READY_ASSET
      → REALTIME_CONSUMER_PROCESSED_ASSET

대응 관계:
  PRODUCER_DONE_ASSET      → REALTIME_PRODUCER_DONE_ASSET
  BROKER_READY_ASSET       → REALTIME_BROKER_READY_ASSET
  CONSUMER_PROCESSED_ASSET → REALTIME_CONSUMER_PROCESSED_ASSET
"""

from airflow.sdk import Asset

# ── 파이프라인 3단계 Asset (기존 welding_assets.py 대응) ─────────────────────

# DataFeeder가 watched/ 폴더에 파일 생성 완료
# (기존: producer.py가 Kafka에 메시지 전송 완료)
REALTIME_PRODUCER_DONE_ASSET = Asset(
    name="welding_realtime_producer_done",
    uri="welding://asset/realtime/producer_done",
)

# FileWatcher가 watched/ 파일을 감지하여 Python queue에 적재 완료
# (기존: Kafka topic에 메시지 존재 + consumer lag 정상)
REALTIME_BROKER_READY_ASSET = Asset(
    name="welding_realtime_broker_ready",
    uri="welding://asset/realtime/broker_ready",
)

# Consumer가 queue에서 처리하여 result CSV 저장 완료
# (기존: Spark Streaming이 pattern_summary 테이블에 저장 완료)
REALTIME_CONSUMER_PROCESSED_ASSET = Asset(
    name="welding_realtime_consumer_processed",
    uri="welding://asset/realtime/consumer_processed",
)

# ── 운영 모니터링 Asset ───────────────────────────────────────────────────────

# 5분 헬스체크 DAG → 정상 상태 신호
REALTIME_PIPELINE_HEALTHY_ASSET = Asset(
    name="welding_realtime_pipeline_healthy",
    uri="welding://asset/realtime/pipeline_healthy",
)

# 일별 리포트 생성 완료 신호
REALTIME_DAILY_REPORT_ASSET = Asset(
    name="welding_realtime_daily_report_done",
    uri="welding://asset/realtime/daily_report_done",
)

# 수동 백필 완료 신호
REALTIME_BACKFILL_DONE_ASSET = Asset(
    name="welding_realtime_backfill_done",
    uri="welding://asset/realtime/backfill_done",
)
