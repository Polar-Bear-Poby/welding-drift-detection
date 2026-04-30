# 6회차 발표용 시나리오 이미지 프롬프트

## 사용 방법
- 아래 프롬프트를 그대로 이미지 생성 도구(예: GPT 이미지 생성, Canva AI)에 입력한다.
- 해상도는 슬라이드용 `1920x1080` 권장.
- 스타일은 통일: `clean industrial dashboard style, white background, blue/green/orange palette`.

---

## 시나리오 A: 정상 부하(기준선)

### 이미지 목적
- “기본 파이프라인이 안정적으로 흐른다”는 인상을 전달

### 프롬프트
`A clean industrial data pipeline illustration for battery welding monitoring. Three production lines feed a producer, then Kafka topics split by laser_a and laser_b, then two Spark consumers process and write to PostgreSQL, and Airflow dashboards monitor success metrics. Show smooth green arrows, low lag, stable throughput indicators. Add labels: Q batteries/day, baseline, E2E latency, completeness 99%+. Modern flat infographic style, 16:9.`

---

## 시나리오 B: 버스트 트래픽

### 이미지 목적
- “짧은 시간 트래픽 급증 -> lag 상승 -> 회복” 패턴 전달

### 프롬프트
`A before-and-after burst traffic infographic for a Kafka-Spark pipeline. Left side: normal flow with low queue. Right side: burst event from production lines causing Kafka backlog spike, then Spark catch-up and recovery. Visualize queue bars rising then falling, lag curve peaking then recovering, no data loss badge. Include labels: burst 2.0x, backlog drain time, stable recovery. Industrial analytics style, high clarity, 16:9.`

---

## 시나리오 C: 채널별 추론 시간 차이

### 이미지 목적
- `laser_a`, `laser_b`의 추론 시간 차이가 지연에 미치는 영향 전달

### 프롬프트
`A comparative latency diagram for two AI inference channels in manufacturing data. Channel A (laser_a) has 80ms inference, Channel B (laser_b) has 220ms inference. Both consume from Kafka and write to PostgreSQL. Show parallel lanes with stopwatch icons and different latency bars. Emphasize that both outputs are normal classification but processing time differs. Add labels: ingest-to-DB latency by channel, bottleneck identification. Clean technical slide illustration, 16:9.`

---

## 시나리오 D: Spark consumer 장애 및 복구

### 이미지 목적
- 장애 감지/자동 복구/백로그 복원 흐름을 한 장에서 설명

### 프롬프트
`A fault recovery workflow graphic for streaming pipeline operations. Sequence: Spark consumer running -> forced kill -> Kafka backlog accumulation -> Airflow monitor detects unhealthy state -> restart task -> consumer resumes with same group/offset -> backlog drains while new data keeps arriving -> DB completeness restored. Use red warning for failure step, yellow for detection, green for recovery. Include labels: MTTD, MTTR, no data loss target. Modern operations dashboard style, 16:9.`

---

## 시나리오 통합 요약 이미지(선택)

### 이미지 목적
- 4개 시나리오를 한 페이지에 요약

### 프롬프트
`A four-panel summary slide for data engineering load and failure testing in battery welding pipeline. Panel 1 baseline stable throughput, panel 2 burst lag and recovery, panel 3 channel inference latency difference, panel 4 failure detection and auto-restart. Common architecture: Kafka, Spark, PostgreSQL, Airflow. Add concise metric chips in each panel (throughput, lag, E2E latency, recovery time). Professional technical presentation style, white background, 16:9.`

