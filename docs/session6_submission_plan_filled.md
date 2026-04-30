# 6회차 제출용 보완 문서 (요구사항 매핑 완료본)

## 1) 부하 시나리오 설계

### 1.1 테스트 목적
- 오늘 할당량 `Q`를 같은 총량으로 유지한 상태에서, 정상/피크/버스트 부하에서 처리 완결성(누락/중복)과 지연 특성을 검증한다.
- 장애 이후 복구 시 backlog를 유실 없이 흡수하는지 확인한다.

### 1.2 로컬 환경 리소스 제약 (실측)
- CPU: `Intel Core Ultra 7 155H`, 16코어 / 22스레드
- RAM: `31.53 GB`
- OS: `Windows 11 Home 64-bit`
- 디스크(C:): 총 `453.67 GB`, 여유 `6.79 GB` (중요 제약)
- 최근 실행 시 docker stats 기준 병목:
  - `welding-spark-worker` CPU 급상승, 메모리 사용 큼
  - Airflow triggerer CPU 비정상 상승 구간 존재

### 1.3 평시/피크 정량 트래픽 측정 방법
- 정의:
  - `Q = 하루 총 배터리 할당량`
  - 배터리 1개당 채널 2개이므로 최종 기대 row = `2Q`
- 입력량 기준:
  - 라인 수 `L`, 라인당 배터리 수 `Q/L` (Q는 L로 나누어지도록 설정)
  - producer replay speed를 조절해 평시/피크/버스트를 만든다.
- 측정 지표:
  - `producer_duration_sec`
  - `time_to_first_db_sec`
  - `end_to_last_db_sec`
  - `db_drain_after_producer_sec`
  - `db_rows / expected_rows`
- 실험 레벨:
  - Baseline: speed=120
  - Peak: speed=220
  - Burst: speed=320

### 1.4 테스트 도구 선택
- 부하 생성: 기존 replay producer + `scripts/measure_p1_c1_stream_timing.sh`
- 시나리오 실행 자동화: `scripts/session6_run_load_tests.sh`
- 장애 시뮬레이션/복구 측정: `scripts/session6_run_failure_tests.sh`

---

## 2) 장애 시나리오 및 대응 전략 (컴포넌트별)

| 컴포넌트 | 장애 시나리오 | 감지 방법 | 자동 복구 | 수동 개입 기준 |
|---|---|---|---|---|
| Kafka | consumer lag 급증 | consumer-group lag, 처리량 감소 | Spark consumer 재기동, 처리량 완화 | lag가 일정 시간 이상 회복 안 될 때 파티션/리소스 조정 |
| Kafka | broker 중단(단일 브로커) | producer send 실패, topic metadata 오류 | 재시작 후 producer 재시도 | 재시작 실패/반복 시 운영자 개입 |
| Spark | streaming consumer 프로세스 중단 | Airflow consumer monitor에서 그룹 멤버 수 미달 | DAG에서 재기동 명령 수행 | 재기동 후에도 그룹 멤버 미달이면 수동 |
| Spark | OOM/CPU 포화로 지연 | 처리 지연 증가, lag 증가, executor 실패 | executor 자원 축소/재분배, consumer 수 조정 | 반복 OOM 시 배치 크기/노드 증설 검토 |
| Airflow | DAG task 실패 | UI 상태/DB dag_run, task_instance | retry/backoff 실행 | retry 소진 후 알림 및 원인 분석 |
| Airflow | SLA 미달 | end_to_last_db_sec 임계 초과 | 경고 알림 + 다음 run 우선순위 조정 | 반복 SLA 위반 시 스케줄/리소스 재설계 |
| PostgreSQL | 쓰기 실패/지연 | write error, DB row 증가 정체 | 재시도, 연결 복구 | 장시간 미복구 시 fallback 저장 후 수동 복구 |

---

## 3) 모니터링 전략 (도구/수집/시각화 결정)

### 3.1 도구 결정
- 1차(지금 로컬): 
  - Docker stats + Airflow UI + Kafka consumer-groups CLI + PostgreSQL query
- 2차(발표 확장안/EC2):
  - Prometheus + Grafana
  - Kafka Exporter / JMX Exporter
  - Airflow 메타DB 기반 지표 수집

### 3.2 수집할 지표
- Kafka:
  - consumer lag
  - topic throughput (in/out)
  - broker health
- Spark:
  - micro-batch 처리 지연
  - failed batch count
  - executor CPU/memory
- Airflow:
  - DAG success/fail rate
  - retry 횟수
  - run duration
- DB:
  - write rows/min
  - write error count

### 3.3 시각화
- Grafana 대시보드 패널:
  - Panel 1: Kafka lag 추이
  - Panel 2: Spark 처리 지연
  - Panel 3: Airflow DAG 상태/실패율
  - Panel 4: DB write throughput + completeness

---

## 4) Fallback / Alert 전략

### 4.1 알림 기준 (무엇을 알릴지)
- Kafka lag > 임계치 N분 지속
- Spark consumer 개수 부족(채널별 기준 미달)
- Airflow DAG 실패 및 retry 소진
- E2E 처리시간(`end_to_last_db_sec`) SLA 초과
- DB write 정체/실패

### 4.2 자동 복구 vs 수동 개입 기준
- 판단 축은 **시점 + 컴포넌트 둘 다** 사용한다.
- 컴포넌트 축:
  - Kafka/Spark/Airflow/DB별 장애 유형이 다르므로 처리 로직 분리
- 시점 축:
  - `T0`: 장애 감지
  - `T0 + t1`: 자동 재시도/재기동 구간
  - `T0 + t2`: 미복구 시 수동 개입 전환
- 예시:
  - Spark consumer 다운 -> 자동 재기동 1회
  - 2~3분 내 정상 복귀 실패 -> 운영자 수동 개입

### 4.3 fallback
- DB 저장 실패 시:
  - 임시 파일(로컬/스토리지)로 적재 실패 payload 보존
  - DB 복구 후 재적재 배치 실행

---

## 5) 실행 가능한 코드/스크립트

### 5.1 부하 시나리오 실행
```bash
bash scripts/session6_run_load_tests.sh \
  --q 120 \
  --line-count 3 \
  --consumer-count 2 \
  --date-folder 20220417 \
  --host-data-dir /mnt/d/metacode_battery_drfit/data_runtime_flat
```

### 5.2 장애 시나리오 실행
```bash
bash scripts/session6_run_failure_tests.sh \
  --expected-consumers 2 \
  --recovery-timeout-sec 300
```

---

## 6) 테스트 결과 섹션 (실험 후 채우기)
- 현재는 템플릿 상태로 유지 (측정 전)
- 결과 입력 위치:
  - `storage/metrics/session6/load_report_*.csv`
  - `storage/metrics/session6/failure_report_*.csv`
- 발표 시 포함할 최소 항목:
  - 정상 vs 피크 vs 버스트 비교표
  - 장애 복구 시간(MTTR) / 감지 시간(MTTD)
  - 병목 분석과 개선안

