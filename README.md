# Spark Batch — Hive External Table + WAU 계산

## 설계 개요

```
data/input/2019-Oct.csv
data/input/2019-Nov.csv
       │
       ▼
[1] CSV 읽기 + UTC→KST 변환
       │
       ▼
[2] 세션 ID 생성 (user_id별 5분 갭 Window 함수)
       │
       ▼
[3] event_date(KST)로 동적 파티셔닝 → Parquet+Snappy 저장
       │
    data/warehouse/user_activity/event_date=2019-10-01/
       │
       ▼
[4] Hive External Table (MSCK REPAIR TABLE)
       │
       ▼
[5] WAU 계산 (user_id 기준 / session_id 기준)
```

## 핵심 설계 결정

| 항목 | 결정 |
|------|------|
| KST 변환 | `from_utc_timestamp(event_time, "Asia/Seoul")` |
| 세션 생성 | Window(partitionBy user_id, orderBy unix_time) + lag + cumsum |
| 파티션 전략 | `partitionOverwriteMode=dynamic` + `overwrite` |
| 복구 장치 | `_SUCCESS` 마커 파일 체크 (이미 완료된 날짜 skip) |
| Hive Table | External + LOCATION 지정 + MSCK REPAIR |

## 파일 변경 목록

| 파일 | 변경 |
|------|------|
| `BatchJob.scala` | 전체 재작성 |
| `docker-compose.yml` | 볼륨 마운트 추가 |
| `build.sbt` | 유지 |
| `Dockerfile` | 유지 |
