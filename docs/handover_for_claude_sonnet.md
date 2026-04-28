# Claude Sonnet 인수인계 문서

작성일: 2026-04-27 (KST)  
프로젝트: `welding-kafka-submission`

## 1) 이번 작업의 핵심 목표
- Kafka 브로커 분산 기능을 살리기 위해 라인 고정 매핑(consumer↔line 고정)을 제거.
- 대신 채널 기준으로만 소비자 그룹을 나눔.
- 규칙:
- 홀수번 컨슈머: `laser_a` 전용
- 짝수번 컨슈머: `laser_b` 전용
- 실제 분산은 Kafka 브로커가 같은 group 내 컨슈머들에게 자동 배분.

## 2) 주요 변경 사항

### A. 스트리밍 소비 전략 변경
- 기존 일부 스크립트에서 사용하던 `LINE_SLOT_INDEX/LINE_SLOT_COUNT` 기반 분산을 기본 운영 경로에서 제거.
- `KAFKA_GROUP_ID`를 `spark_streaming.py`에서 지원하도록 반영되어, 채널별 소비자 그룹 분리가 가능해짐.
- 현재 표준 운영 규칙:
- `laser_a` 컨슈머 그룹: `welding-stream-laser-a`
- `laser_b` 컨슈머 그룹: `welding-stream-laser-b`

### B. 재시작/운영 스크립트 정렬
- `scripts/start_always_on_pipeline.sh`
- 컨슈머를 1..N 순회해 기동.
- 홀수 id는 `welding.raw.laser_a.v1` + `CHANNEL_FILTER=1` + `KAFKA_GROUP_ID=welding-stream-laser-a`.
- 짝수 id는 `welding.raw.laser_b.v1` + `CHANNEL_FILTER=0` + `KAFKA_GROUP_ID=welding-stream-laser-b`.
- `CONSUMER_COUNT`는 짝수 강제.

- `scripts/measure_p1_c1_stream_timing.sh`
- 벤치마크 실행 시 동일한 홀수/짝수 채널 규칙 사용.
- 벤치마크 종료 후 기본 스트리밍 복구 로직도 같은 규칙으로 통일.

- `airflow/dags/welding_streaming_monitor.py`
- 재시작 Bash 명령을 홀수/짝수 + 채널별 그룹 방식으로 교체.

### C. 토픽 관련 복구 조치
- 컨슈머가 `UnknownTopicOrPartitionException`으로 종료되는 문제 확인.
- 원인: 기본 토픽 `welding.raw.laser_a.v1`, `welding.raw.laser_b.v1` 미존재.
- 조치: 두 토픽 생성 완료.

## 3) 현재 운영 상태 (마지막 확인 기준)
- 코어 컨테이너 실행 중.
- Spark 스트리밍 컨슈머 6개 실행 확인.
- 구독 로그 확인 결과:
- consumer 1/3/5 -> `welding.raw.laser_a.v1`, group `welding-stream-laser-a`
- consumer 2/4/6 -> `welding.raw.laser_b.v1`, group `welding-stream-laser-b`
- always-on daemon 실행 중.

## 4) 관련 파일
- `spark_streaming.py`
- `scripts/start_always_on_pipeline.sh`
- `scripts/measure_p1_c1_stream_timing.sh`
- `airflow/dags/welding_streaming_monitor.py`

## 5) 실행/검증 명령 (PowerShell)

### 기본 상시 파이프라인 기동/복구
```powershell
bash scripts/start_always_on_pipeline.sh
```

### 상시 파이프라인 중지
```powershell
bash scripts/stop_always_on_pipeline.sh
```

### 기본 토픽 존재 확인
```powershell
docker exec welding-kafka kafka-topics --bootstrap-server kafka:9092 --list | Select-String "welding.raw.laser"
```

### 스트리밍 컨슈머 개수 확인
```powershell
docker exec welding-spark-master bash -lc "ps -ef | grep 'python3 /opt/spark/apps/spark_streaming.py' | grep -v grep | wc -l"
```

### Airflow DAG import 에러 확인
```powershell
docker exec welding-airflow-webserver airflow dags list-import-errors
```

## 6) 측정 스크립트 관련 주의사항
- `measure_p1_c1_stream_timing.sh`는 `EXPECTED_ROWS`를 기준으로 DB 안정 구간을 기다림.
- 현재 기본 계산식은 `MAX_PRODUCTS * LINE_COUNT * 2`.
- 실제 데이터 특성/입력 조건과 불일치하면 오래 대기(타임아웃까지)할 수 있음.
- 필요 시 실행 시점에 `EXPECTED_ROWS`를 명시 오버라이드 권장.

예시:
```powershell
bash -lc "LINE_COUNT=3 CONSUMER_COUNT=6 MAX_PRODUCTS=3 EXPECTED_ROWS=9 DATE_FOLDER=20220417 bash scripts/measure_p1_c1_stream_timing.sh"
```

## 7) 다음 담당자에게 권장 작업
- 측정 스크립트의 `EXPECTED_ROWS` 계산을 데이터 생성 규칙과 정확히 맞추기.
- `start_always_on_pipeline.sh`의 기존 `kill: No such process` 경고는 기능상 치명적이지 않지만, 로그 노이즈 감소를 위해 정리 가능.
- (선택) 채널별 컨슈머 개수/처리량을 PostgreSQL 또는 Prometheus로 수집해 자동 튜닝 근거 확보.

---

## 8) 추가 구현된 Airflow DAG (2026-04-28)

총 6개 DAG 신규 구현 완료. 모두 Python 문법 검사 통과.

| 파일명 | 스케줄 | 역할 |
|---|---|---|
| `welding_consumer_health_monitor.py` | `*/5 * * * *` | Spark 컨슈머 6개(laser_a×3, laser_b×3) 생존 확인 + 자동 재기동 |
| `welding_data_availability_check.py` | `30 0 * * *` | 전일 데이터 가용성 검증 → daily_report 트리거 또는 skip alert |
| `welding_batch_backfill.py` | `None` (수동) | 날짜 지정 소급 재처리 (force_overwrite 옵션 포함) |
| `welding_weekly_drift_trend.py` | `0 9 * * MON` | 7일 이동평균 + REGR_SLOPE 기반 장기 드리프트 감지 |
| `welding_kafka_topic_health.py` | `0 * * * *` | 토픽 존재 여부 + Consumer Lag 모니터링 (자동 토픽 재생성) |
| `welding_storage_cleanup.py` | `0 3 * * *` | Parquet(30일), 로그(14일), 체크포인트(7일) 자동 정리 |

### 추가된 DB 테이블
- `welding.weekly_trend`: 주간 드리프트 트렌드 결과 저장 (`schema.sql`에도 반영)

### Airflow DAG import 에러 확인
```powershell
docker exec welding-airflow-webserver airflow dags list-import-errors
```

### 백필 DAG 수동 실행 예시 (Airflow UI 또는 CLI)
```powershell
docker exec welding-airflow-webserver airflow dags trigger welding_batch_backfill `
  --conf '{"target_date":"20220417","line_count":3,"line_seeds":"42,73,128","force_overwrite":false}'
```

