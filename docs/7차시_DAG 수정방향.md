# 7차시 DAG 수정 방향 분석

작성일: 2026-05-04  
교차검토: Claude Sonnet 4.6 → ChatGPT 5.3 Codex 검증 완료 (2026-05-04)  
기준 브랜치: 7차시 구현 완료 시점  
목적: 두 모델의 의견을 종합한 최종 DAG 수정 방향 문서

---

## 핵심 전제: 아키텍처 변경 사항

7차시에서 확정된 **가장 중요한 설계 변경**:

| 구분 | 이전 방식 (결함) | 현재 방식 (확정) |
|---|---|---|
| 프로듀서 역할 | 프로듀서 1개 → N개 라인 데이터를 순차 전송 | 프로듀서 1개 = 생산라인 1대 |
| 병렬성 | 라인 간 블로킹 발생 | 컨테이너 N개 = 라인 N대, 완전 병렬 |
| 불변식 | 없음 | `producer_count == line_count` 항상 성립 |
| 데이터 단위 | 불명확 | 프로듀서 1개가 laser_a + laser_b 를 동시 전송 |

이 전제가 9개 DAG 각각에 어떤 영향을 주는지 아래에 정리한다.

---

## DAG별 분석

### 1. `welding_batch_ingest` — ⚠️ 수정 필요

**현재 동작**  
```python
REPLAY_LINE_COUNT = int(os.getenv("REPLAY_LINE_COUNT", "3"))
REPLAY_LINE_SEEDS = os.getenv("REPLAY_LINE_SEEDS", "42,73,128")

run_producer = BashOperator(
    bash_command=(
        f"docker exec {PRODUCER_CONTAINER} python /app/producer.py "
        f"--line-count {REPLAY_LINE_COUNT} --line-seed \"{REPLAY_LINE_SEEDS}\" ..."
    )
)
```

**문제점**  
- `PRODUCER_CONTAINER = "welding-producer"` → **단일 컨테이너 하나만** 실행함
- 새 아키텍처에서는 라인 N대 = 프로듀서 컨테이너 N개여야 함
- 현재 DAG는 `welding-producer` 한 개에 `--line-count 3`을 넘겨 순차 실행 → 이전 방식 그대로

**수정 방향**
- `run_producer` Task를 단일 컨테이너 실행 → **라인 수만큼 프로듀서 컨테이너 기동**으로 교체
  - 예: `for i in 1..N: docker exec welding-producer-{i} ...` 
  - 또는 `docker compose up --scale producer=N` 방식 검토
- `REPLAY_LINE_COUNT` 환경변수 → 프로듀서 컨테이너 수와 1:1로 매핑되도록 명확히 문서화
- `validate_results` Task의 검증식:
  ```python
  # 현재: ERROR 행 비율 검사 (quality_decision = 'ERROR')
  COUNT(*) FILTER (WHERE quality_decision = 'ERROR')
  ```
  7차시에서 `quality_decision`이 `"drift"` / `"normal"`로 변경됨 → **`'ERROR'` 필터가 영구적으로 0을 반환**
  ```sql
  -- 수정 필요
  COUNT(*) FILTER (WHERE quality_decision = 'drift')
  ```

---

### 2. `welding_batch_backfill` — ⚠️ 수정 필요 (validate_results)

**현재 동작**  
```python
cur.execute(
    "SELECT COUNT(*), COUNT(*) FILTER (WHERE quality_decision = 'ERROR') "
    "FROM welding.pattern_summary "
    "WHERE run_id = %s AND event_date = %s",
    ...
)
error_rate = errors / total
if error_rate >= 0.5:
    raise ValueError(...)
```

**문제점**  
- `'ERROR'` 필터가 `"drift"` / `"normal"` 체계에서 **영구 0 반환** → 즉시 수정 필요
- `consumer_count` 파라미터가 `line_count`와 별개로 존재:
  ```python
  "line_count": Param(default=3, ...),
  "consumer_count": Param(default=6, ...),
  ```
  이것은 **즉시 버그는 아님** — DAG 내부 검증 로직에서 직접 사용되지 않고, 설명/운영 규약 수준의 혼동 소지임 (ChatGPT Codex 판정: 설계/명명 문제)

**수정 방향**
- **(필수)** `validate_results`의 `'ERROR'` 필터 → `'drift'`로 변경, 에러율 개념 재정의:
  - `quality_decision = 'drift'` 건수를 정보로 기록, 경보 조건은 별도 설정
- **(권고)** `consumer_count` 파라미터 이름을 `spark_consumer_count`로 변경하거나 주석으로 의미 명확화

---

### 3. `welding_consumer_health_monitor` — ⚠️ 수정 필요

**현재 동작**  
```python
_REQUESTED_CONSUMER_COUNT = int(os.getenv("CONSUMER_COUNT", "6"))
# line_count=3 → consumer_count=6 (라인당 2개: laser_a용 1 + laser_b용 1)
DEFAULT_EXPECTED_PER_CHANNEL = max(1, TARGET_CONSUMER_COUNT // 2)
EXPECTED_LASER_A = int(os.getenv("EXPECTED_LASER_A", str(DEFAULT_EXPECTED_PER_CHANNEL)))
EXPECTED_LASER_B = int(os.getenv("EXPECTED_LASER_B", str(DEFAULT_EXPECTED_PER_CHANNEL)))
```

**문제점**  
- 이 DAG는 Spark consumer(스트리밍 컨슈머)를 모니터링함 → **프로듀서 수와는 무관**
- Spark consumer는 여전히 2개 그룹(`welding-stream-laser-a`, `welding-stream-laser-b`)으로 동작
- 이 부분 자체는 새 아키텍처와 충돌 없음
- **단, `RESTART_CMD`의 프로세스 수 계산이 `CONSUMER_COUNT` 기반이어서,**  
  `CONSUMER_COUNT` 환경변수가 "프로듀서 컨테이너 수"와 혼동될 위험 있음

**수정 방향**
- `CONSUMER_COUNT` 환경변수 이름을 `SPARK_CONSUMER_COUNT`로 변경하거나  
  주석에 "이 값은 Spark Streaming consumer 수이며 생산라인 수(producer_count)와 무관함"을 명확히 기술
- 실질적인 로직 변경은 필요 없음

---

### 4. `welding_daily_quality_report` — ⚠️ 수정 필요 (SQL 집계 로직)

**현재 동작**  
```sql
WITH today AS (
    SELECT ...
           COUNT(*) FILTER (WHERE quality_decision = 'PASS')   as pass,
           COUNT(*) FILTER (WHERE quality_decision = 'REVIEW') as review,
           COUNT(*) FILTER (WHERE quality_decision = 'ERROR')  as error,
    ...
)
INSERT INTO welding.daily_report (
    ..., pass_count, review_count, error_count, ...
)
```

**문제점**  
- `quality_decision` 값이 `"PASS"` / `"REVIEW"` / `"ERROR"` 기준으로 집계
- 7차시 이후 실제 저장 값은 `"normal"` / `"drift"`
- → `pass_count`, `review_count`, `error_count` 가 **모두 0**이 됨
- `daily_report` 테이블의 컬럼 이름 자체(`pass_count`, `review_count`, `error_count`)도 새 체계와 불일치

**수정 방향**

**옵션 A (최소 수정)**: SQL 필터만 교체
```sql
COUNT(*) FILTER (WHERE quality_decision = 'normal') as pass_count,
COUNT(*) FILTER (WHERE quality_decision = 'drift')  as drift_count,
0                                                   as review_count,  -- 임시 유지
0                                                   as error_count    -- 임시 유지
```

**옵션 B (정석)**: `daily_report` 테이블 컬럼 재설계
```sql
-- daily_report 테이블 변경
normal_count  INTEGER NOT NULL DEFAULT 0,  -- pass_count 대체
drift_count   INTEGER NOT NULL DEFAULT 0,  -- error_count 대체
-- review_count, error_count 삭제
```
→ `schema.sql`과 `welding_weekly_drift_trend.py`의 `pass_rate` 계산도 함께 수정 필요

> **권고**: 옵션 A로 먼저 적용하고, 향후 실제 모델이 붙을 때 옵션 B로 전환

---

### 5. `welding_weekly_drift_trend` — ⚠️ 수정 필요 (pass_rate 계산)

**현재 동작**  
```sql
CASE WHEN SUM(total_products) > 0
     THEN SUM(pass_count)::float / SUM(total_products)
     ELSE NULL END AS pass_rate
```

**문제점**  
- `daily_report.pass_count`가 위 4번 문제로 인해 항상 0이므로 `pass_rate = 0.0`이 됨
- 장기 드리프트 판정(`long_term_drift`)은 `avg_cpd_score`와 `cpd_trend_slope` 기반이므로 큰 영향은 없음
- 단, 발표 시 `pass_rate`가 0%로 보이면 설명 필요

**수정 방향**
- `daily_report`에서 옵션 A를 선택한 경우: `pass_rate = SUM(normal_count) / SUM(total_products)` 로 교체
- `long_term_drift` 판정 로직 자체는 변경 불필요

---

### 6. `welding_data_availability_check` — ✅ 변경 불필요

**분석**  
```python
MIN_EXPECTED_ROWS = 6  # 라인 3개 × 채널 2개
```

- 이 DAG는 `pattern_summary` 행 수만 카운트 → `quality_decision` 값과 무관
- 라인 수가 늘어나면 `MIN_EXPECTED_ROWS` 환경변수화를 고려할 수 있으나 필수 아님
- `line_count`, `channel_count` 도 조회해서 heartbeat에 기록하므로 정보 가치 있음

**권고 (선택)**  
- `MIN_EXPECTED_ROWS`를 환경변수 `MIN_EXPECTED_SUMMARY_ROWS=6`으로 외부화하면  
  라인 수 변경 시 코드 수정 없이 대응 가능

---

### 7. `welding_kafka_topic_health` — 🟡 운영 토픽 전략 정합성 점검 필요

> **ChatGPT Codex 수정 판정**: Claude의 "변경 불필요" 단정은 과함. DAG 자체는 쓸 만하나, 운영 토픽 전략과의 정합성 점검이 필요.

**분석**  
- 모니터링 대상: `welding.raw.v1`, `welding.raw.laser_a.v1`, `welding.raw.laser_b.v1` (v1 고정)
- 프로듀서 N개 아키텍처에서 동일 토픽에 publish하는 구조이므로 기본 v1 모드에서는 유효
- **단**, benchmark 동적 토픽(`welding.raw.laser_a.benchmark...` 등)을 병용하는 운영 모드에서는  
  이 DAG가 해당 토픽을 모니터링하지 않아 **사각지대** 발생 가능

**수정 방향**
- **(권고)** `REQUIRED_TOPICS`를 환경변수 또는 config 목록으로 외부화하여 토픽 전략 변경 시 코드 수정 없이 대응
- **(권고)** `LAG_ALERT_THRESHOLD = 10000` 환경변수화
- **(선택)** 운영 모드(v1 고정 vs benchmark 혼용)에 따라 모니터링 토픽 목록을 분기하는 로직 추가

---

### 8. `welding_storage_cleanup` — ✅ 변경 불필요

**분석**  
- Parquet 파일, 로그, Spark 체크포인트 정리 → `quality_decision` 값 무관
- 프로듀서 수와도 무관
- 현재 로직 완전 유효

---

### 9. `welding_streaming_monitor` — ⚠️ 수정 필요 (강함)

> **ChatGPT Codex 수정 판정**: `source_file` 불일치가 실제 DB에서 확인됨. 오탐/불필요 재시작 유발 가능. 우선 수정 대상.

**현재 동작**  
```python
SOURCE_LASER_A = f"kafka://{TOPIC_RAW_LASER_A}"  # "kafka://welding.raw.laser_a.v1"
SOURCE_LASER_B = f"kafka://{TOPIC_RAW_LASER_B}"  # "kafka://welding.raw.laser_b.v1"

# pattern_summary에서 이 URI로 source_file을 필터링
cur.execute(
    "SELECT source_file, COUNT(*) FROM welding.pattern_summary
     WHERE ... AND source_file IN (%s, %s)",
    (SOURCE_LASER_A, SOURCE_LASER_B),
)
```

**확인된 문제**  
- 실제 DB의 `pattern_summary.source_file`에는 CSV 파일 경로 또는  
  `kafka://welding.raw.laser_a.benchmark...` 형태의 값이 저장되어 있음
- 고정된 `kafka://welding.raw.laser_a.v1` 문자열과 불일치 → **헬스체크가 항상 0 반환**
- 결과: `a_count=0, b_count=0` → 항상 `restart_spark_streaming` 브랜치로 분기 → **불필요한 재시작 반복**
- `CONSUMER_COUNT = LINE_COUNT * 2` 공식도 새 아키텍처(producer_count=line_count, Spark consumer는 채널 기반)와 의미 혼재

**수정 방향**  
- **(필수)** `_recent_channel_counts`의 헬스체크 기준을 `source_file` 고정 URI → `processed_at` 시간 기반으로 교체:
  ```python
  # 수정안: source_file 필터 제거, 채널 구분은 channel 컬럼 사용
  SELECT channel, COUNT(*)
  FROM welding.pattern_summary
  WHERE processed_at >= NOW() - INTERVAL '{window_minutes} minutes'
  GROUP BY channel
  # channel=1 → laser_a, channel=0 → laser_b
  ```
- **(권고)** `CONSUMER_COUNT` 변수 이름을 `SPARK_CONSUMER_COUNT`로 변경하고 `LINE_COUNT`와 독립적으로 설정

---

## 전체 요약표 (Claude + ChatGPT Codex 교차검토 최종)

| DAG | 최종 판정 | 핵심 수정 사항 | 우선순위 |
|---|---|---|---|
| `welding_batch_ingest` | ⚠️ **수정(강함)** | ① 프로듀서 다중 기동 방식 변경 ② `'ERROR'` → `'drift'` 필터 수정 | **즉시** |
| `welding_batch_backfill` | ⚠️ **부분 수정** | ① `'ERROR'` 필터 수정 (즉시 버그) ② `consumer_count` 명명 정리 (설계) | **높음** |
| `welding_consumer_health_monitor` | 🟡 **명명 정리** | `CONSUMER_COUNT` → 주석/이름으로 Spark consumer임을 명확화 | 낮음 |
| `welding_daily_quality_report` | ⚠️ **수정(강함)** | SQL 집계를 `'normal'`/`'drift'` 기준으로 변경 (리포트 왜곡 방지) | **즉시** |
| `welding_weekly_drift_trend` | ⚠️ **수정** | `pass_rate` 계산식 → `normal_count` 기준으로 변경 | **높음** |
| `welding_data_availability_check` | ✅ **유지** | (권고) `MIN_EXPECTED_ROWS` 환경변수화 | 선택 |
| `welding_kafka_topic_health` | 🟡 **토픽 전략 점검** | 운영 토픽(v1 vs benchmark) 정합성 확인, REQUIRED_TOPICS 외부화 | 중간 |
| `welding_storage_cleanup` | ✅ **유지** | 없음 | — |
| `welding_streaming_monitor` | ⚠️ **수정(강함)** | `source_file` 필터를 `channel` 컬럼 + `processed_at` 기반으로 교체 | **즉시** |

---

## ChatGPT 5.3 Codex 교차검토 결과 (2026-05-04)

### 판정 요약

| DAG | Claude 초안 | ChatGPT Codex 최종 판정 |
|---|---|---|
| `welding_batch_ingest` | ⚠️ 수정 필요 | ✅ **수용(강함)** — 단일 컨테이너 구조 + ERROR 필터 모두 타당 |
| `welding_batch_backfill` | ⚠️ 수정 필요 | 🟡 **부분 수용** — ERROR 필터는 즉시 버그, consumer_count는 설계/명명 문제 |
| `welding_consumer_health_monitor` | ⚠️ 문서 수정 | 🟡 **부분 수용** — 로직은 동작 가능, 이름/운영 규약 정리 수준 |
| `welding_daily_quality_report` | ⚠️ 수정 필요 | ✅ **수용(강함)** — 실제 리포트 왜곡 핵심 이슈 |
| `welding_weekly_drift_trend` | ⚠️ 수정 필요 | ✅ **수용** — daily 집계 변경 시 pass_rate 함께 수정 필요 |
| `welding_data_availability_check` | ✅ 변경 불필요 | 🟡 **대체로 유지** — MIN_EXPECTED_ROWS env화 보완 권장 |
| `welding_kafka_topic_health` | ✅ 변경 불필요 | ⚠️ **부분 반박** — "완전 유지"는 과함, 운영 토픽 전략 점검 필요 |
| `welding_storage_cleanup` | ✅ 변경 불필요 | ✅ **수용** — 유지 가능 |
| `welding_streaming_monitor` | 🟡 부분 검토 | ✅ **수용(강함)** — source_file 불일치 DB에서 실제 확인됨, 우선 수정 |

### 핵심 추가 정보 (ChatGPT Codex 제공)

**`welding_streaming_monitor` source_file 불일치 확인**  
실제 DB의 `pattern_summary.source_file`에 `kafka://welding.raw.laser_a.benchmark...` 형태의 값이 존재함.  
DAG의 `SOURCE_LASER_A = "kafka://welding.raw.laser_a.v1"` 고정값과 불일치 → **헬스체크가 항상 0 반환 중**.  
→ 불필요한 Spark 재시작이 반복되고 있을 가능성이 높음. **즉시 수정 대상**.

**`welding_kafka_topic_health` 운영 모드 사각지대**  
benchmark 동적 토픽을 병용하는 모드에서 이 DAG가 해당 토픽을 모니터링하지 않음.  
→ "변경 불필요"가 아니라 운영 토픽 전략 확정 후 `REQUIRED_TOPICS` 목록 점검 필요.

---

## Q1~Q4 확정 답변 (ChatGPT 5.3 Codex, 2026-05-04)

### Q1. 프로듀서 다중 기동 방식 → **DAG Task Mapping 방식 (A·B 아님)**

**확정**: `--scale`(A)도 사전정의(B)도 아닌 **DAG에서 라인별 Task를 병렬 생성**하는 방식.

**근거**
- `docker-compose.yml`의 `producer` 서비스는 `container_name: welding-producer` 고정이라 `--scale`을 바로 적용할 수 없음
- B는 라인 수가 늘어날 때마다 compose 파일을 수동 수정해야 해 유지보수 비용이 큼
- **DAG Task Mapping**: 각 Task가 `--line-number {i}` 등을 받아 독립 실행 → `producer_count == line_count` 불변식을 DAG 레벨에서 가장 깔끔하게 만족

**수정 방향 (`welding_batch_ingest.py`)**
```python
# 현재: 단일 BashOperator로 welding-producer 1개 호출
# 수정: dynamic task mapping으로 라인별 Task 병렬 생성

@task()
def run_producer_for_line(line_number: int):
    import subprocess
    cmd = (
        f"docker exec welding-producer python /app/producer.py "
        f"--data-dir /data --kafka kafka:9092 "
        f"--line-number {line_number} "
        f"--speed {REPLAY_SPEED} --no-schedule-wait --oldest-date-only"
    )
    result = subprocess.run(cmd, shell=True, timeout=1800)
    if result.returncode != 0:
        raise RuntimeError(f"producer failed for line={line_number}")

line_numbers = list(range(1, REPLAY_LINE_COUNT + 1))
run_producer_for_line.expand(line_number=line_numbers)
```

---

### Q2. `quality_decision` 값 체계 — **지금은 병행(`IN`), 이후 일괄 마이그레이션**

**확정**: 즉시 마이그레이션이 아닌 **`IN ('PASS', 'normal')` 병행 처리** 먼저.

**근거**
- `spark_batch.py` (배치): 7차시부터 `"normal"` / `"drift"` 저장 (L379)
- `spark_streaming.py` (스트리밍): 아직 `"PASS"` 기록 중 (L106)
- → **현재 파이프라인이 혼재 상태** → 즉시 마이그레이션만으로 해결 불가

**수정 방향 (daily/weekly SQL)**
```sql
-- 정상 판단: PASS(스트리밍 레거시) + normal(배치 신규) 모두 포함
COUNT(*) FILTER (WHERE quality_decision IN ('PASS', 'normal')) AS pass_count,
COUNT(*) FILTER (WHERE quality_decision = 'drift')            AS drift_count,
```

**후속 마이그레이션 조건** (spark_streaming.py 수정 완료 후)
```sql
UPDATE welding.pattern_summary
SET quality_decision = 'normal'
WHERE quality_decision = 'PASS';
```

---

### Q3. `daily_report` 컬럼 재설계 → **단기 옵션 A, 중기 옵션 B**

| 단계 | 방법 | 시점 |
|---|---|---|
| **단기 (A)** | SQL 필터만 수정. `pass_count ← IN('PASS','normal')`, `error_count ← 'drift'`로 매핑 | 즉시 |
| **중기 (B)** | `ALTER TABLE daily_report` + `normal_count`, `drift_count` 컬럼 정식 추가. `weekly_trend`의 `pass_rate`도 함께 수정 | 실제 모델 적용 후 |

**근거**: 현재 daily SQL이 `PASS/REVIEW/ERROR` 기준이라 집계가 모두 0이 되는 현실적 왜곡 위험 ([welding_daily_quality_report.py L122](welding_daily_quality_report.py))

---

### Q4. `welding_streaming_monitor` 헬스체크 → **`channel` 컬럼 + `processed_at` 기반으로 교체**

**확정**: `source_file` URI 매칭 방식 폐기 → `channel` 컬럼(0/1) + 시간 윈도우 기반으로 전환.

**근거**: DAG는 `kafka://welding.raw.laser_a.v1`만 찾는데 ([welding_streaming_monitor.py L31](welding_streaming_monitor.py)),
실제 DB에는 `kafka://welding.raw.laser_a.benchmark....` 형태로 저장됨 (실측 확인).
→ 헬스체크가 항상 0 반환 → 항상 `restart_spark_streaming` 분기 → **불필요한 재시작 반복 중**.

**수정 방향**
```python
def _recent_channel_counts(window_minutes: int) -> tuple[int, int]:
    with psycopg2.connect(DB_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT channel, COUNT(*)
                FROM welding.pattern_summary
                WHERE processed_at >= NOW() - (%s || ' minutes')::interval
                GROUP BY channel
                """,
                (window_minutes,),
            )
            rows = cur.fetchall()
    counts = {0: 0, 1: 0}  # 0=laser_b, 1=laser_a
    for channel, count in rows:
        counts[int(channel)] = int(count)
    return counts[1], counts[0]  # (laser_a_count, laser_b_count)
```

---

## 🆕 추가 발견: `spark_batch.py:433` — drift 추출 조건 버그

> **출처**: ChatGPT 5.3 Codex 코드 대조 중 발견

**위치**: `spark_batch.py`, `write_drift_artifacts` 함수

```python
# 현재 (버그)
drift_summary_df = summary_df.filter(
    F.col("quality_decision") != F.lit("PASS")
).cache()
```

**문제**
- 7차시에서 `quality_decision`이 `"normal"` / `"drift"`로 변경됨
- `!= 'PASS'` 조건 → `"normal"`과 `"drift"` 모두 해당 → **정상 배터리 전량이 drift 파일로 추출**됨

**영향**
- `drift_detected/` 폴더에 정상 데이터까지 복사 → 스토리지 낭비
- `drift_summary_rows`, `drift_segment_rows` 통계가 과장됨 (로그 오도)
- 향후 실제 모델 재학습 시 노이즈 데이터 포함 위험

**수정** (`spark_batch.py L432`)
```python
# 수정
drift_summary_df = summary_df.filter(
    F.col("quality_decision") == F.lit("drift")
).cache()
```

**우선순위**: 🔴 **즉시 수정** — 현재 배치 결과의 drift 아티팩트 전체가 잘못 추출되고 있음.

---

## 최종 수정 액션 플랜

| 순서 | 대상 파일 | 수정 내용 | 긴급도 |
|---|---|---|---|
| 1 | `spark_batch.py` L432 | `!= 'PASS'` → `== 'drift'` (drift 추출 조건 버그) | 🔴 즉시 |
| 2 | `welding_streaming_monitor.py` | `source_file` 필터 → `channel` 컬럼 + `processed_at` 기반으로 교체 | 🔴 즉시 |
| 3 | `welding_daily_quality_report.py` | SQL 집계 필터를 `IN ('PASS','normal')` / `'drift'` 기준으로 수정 | 🔴 즉시 |
| 4 | `welding_batch_ingest.py` | `validate_results`의 `'ERROR'` → `'drift'` 필터 수정 | 🟠 높음 |
| 5 | `welding_batch_backfill.py` | 동일 `'ERROR'` → `'drift'` 필터 수정 | 🟠 높음 |
| 6 | `welding_weekly_drift_trend.py` | `pass_rate` → `SUM(pass_count)` 기준 수정 (Q2 병행처리 반영) | 🟠 높음 |
| 7 | `welding_batch_ingest.py` | 단일 BashOperator → DAG Task Mapping으로 프로듀서 병렬화 | 🟡 중간 |
| 8 | `spark_streaming.py` | `quality_decision` 값을 `"PASS"` → `"normal"`로 통일 (마이그레이션 전제 조건) | 🟡 중간 |

