# Backpacker: Spark Batch Job for E-Commerce User Activity Analysis

이커머스 사용자 활동 데이터를 처리하여 **WAU(Weekly Active Users)** 를 계산하는 **Apache Spark 배치 애플리케이션**입니다.  
데이터 변환부터 Hive 외부 테이블 관리, 메트릭 계산까지 전 과정을 자동화합니다.

---

## 📋 목차

1. [프로젝트 개요](#프로젝트-개요)
2. [기술 스택 선택 이유](#기술-스택-선택-이유)
3. [프로젝트 구조](#프로젝트-구조)
4. [아키텍처](#아키텍처)
5. [빌드 및 실행](#빌드-및-실행)
6. [기술 상세 설명](#기술-상세-설명)
7. [데이터 흐름](#데이터-흐름)

---

## 📌 프로젝트 개요

### 목표
- CSV 형식의 이커머스 이벤트 데이터(수백만 건~수십억 건)를 효율적으로 처리
- **UTC → KST 변환**, **세션 ID 생성**, **파티셔닝** 등 복잡한 데이터 변환
- **Hive 외부 테이블** 을 통한 구조화된 데이터 관리
- **WAU 메트릭** 자동 계산 및 결과 CSV 내��내기

### 핵심 기능
```
CSV 데이터 → UTC/KST 변환 → 세션 ID 생성 → Parquet 저장 
→ Hive 외부 테이블 등록 → WAU 계산 → CSV 출력
```

---

## 🛠️ 기술 스택 선택 이유

### 1️⃣ **왜 Scala로 Spark 애플리케이션을 작성했나?**

#### Scala 선택 이유
- **Spark의 기본 언어**: Spark는 Scala로 작성되었고, Scala API가 가장 최적화되어 있음
  - Python PySpark보다 **2~3배 빠름**
  - Java보다 **문법이 간결하고 함수형 프로그래밍 지원**

- **타입 안정성**: Scala의 정적 타입 시스템이 대규모 데이터 처리에서 런타임 에러를 사전에 방지
  ```scala
  // 컴파일 시 타입 체크
  val df: DataFrame = spark.read.csv("...")
  df.filter($"price" > 100)  // 타입 안전
  ```

- **함수형 프로그래밍**: 불변 객체와 람다 표현식으로 병렬 처리에 최적화
  ```scala
  val sessionIds = generateSessionIds(kstDf)  // 함수형 변환
  ```

- **Interop**: 필요시 Java 라이브러리를 직접 사용 가능 (Hadoop FileSystem 등)

#### 다른 언어와의 비교

| 언어 | 장점 | 단점 |
|------|------|------|
| **Scala** | Spark 최적화, 타입 안전, 함수형 | 학습곡선 높음 |
| **Python (PySpark)** | 배우기 쉬움, 데이터과학 생태계 | 속도 느림 (Pickle 직렬화) |
| **Java** | 성능 좋음 | 코드 양이 많음, 보일러플레이트 |
| **SQL** | 간단함 | 복잡한 로직 표현 불가, 재사용성 낮음 |

---

### 2️⃣ **왜 SBT를 빌드 도구로 사용하나?**

#### SBT(Scala Build Tool)

**SBT는 Scala 생태계의 표준 빌드 도구**입니다.

##### 주요 기능
```scala
// build.sbt: 프로젝트 설정 파일

name := "backpacker"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
)
```

| 항목 | 설명 |
|------|------|
| `%% "spark-core"` | `%%` = Scala 버전 자동 추가 (2.12) |
| `% "provided"` | Spark는 클러스터에 이미 설치됨 (JAR에 포함 X) |
| `spark-sql`, `spark-hive` | DataFrame, Hive 메타스토어 지원 |

**Maven/Gradle vs SBT**
- Maven: XML 기반, 보일러플레이트 많음
- Gradle: 좋지만 Scala 커뮤니티에서 표준 아님
- **SBT**: Scala로 작성되어 있고, Scala 프로젝트에 최적화 ✅

---

### 3️⃣ **왜 JAR(Java Archive) 파일로 배포하나?**

#### 일반적인 배포 방식

```
Scala 소스코드
    ↓
SBT 컴파일 (build.sbt 설정대로)
    ↓
Java 바이트코드 (.class 파일)
    ↓
ZIP 포맷으로 패킹
    ↓
JAR 파일 (application.jar)
```

#### JAR 파일의 장점

| 장점 | 설명 |
|------|------|
| **배포 용이** | 단일 파일로 애플리케이션 전체 포함 |
| **의존성 해결** | Fat JAR은 모든 라이브러리를 포함 |
| **JVM 실행** | Java가 설치된 어느 곳에서나 실행 가능 |
| **버전 관리** | JAR 하나 = 배포된 정확한 버전 기록 |

#### Fat JAR이란?

일반 JAR은 외부 의존성을 포함하지 않아 실행이 어렵습니다.

```
❌ 일반 JAR
  ├── backpacker/
  └── manifest (entry point만 기록)
  → 실행 시 classpath에서 spark-core.jar, spark-sql.jar 필요

✅ Fat JAR (Assembly)
  ├── backpacker/
  ├── org/apache/spark/
  ├── org/apache/hadoop/
  └── 수백 개의 의존성 라이브러리
  → 단일 JAR만으로 즉시 실행 가능
```

#### build.sbt에서 Fat JAR 설정

```scala
assembly / mainClass        := Some("com.example.BatchJob")
assembly / assemblyJarName  := "backpacker-assembly-0.1.0.jar"

// 충돌 파일 처리: 여러 JAR에서 같은 파일이 나올 때
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")    => MergeStrategy.discard
  case PathList("META-INF", xs @ _*)          => MergeStrategy.discard
  case "reference.conf"                       => MergeStrategy.concat  // 설정 파일 병합
  case _                                      => MergeStrategy.first   // 첫 번째 파일 선택
}
```

이 설정으로 `sbt assembly` 실행 시 모든 의존성이 포함된 **backpacker-assembly-0.1.0.jar** 생성됨.

---

## 📁 프로젝트 구조

```
backpacker/
├── build.sbt                              # Scala/SBT 설정 파일
├── Dockerfile                             # 멀티스테이지 빌드 설정
├── docker-compose.yml                    # 로컬 테스트 환경
├── README.md                              # 이 파일
├── project/
│   └── build.properties                   # SBT 버전
├── src/
│   └── main/scala/com/example/
│       └── BatchJob.scala                 # 메인 배치 로직 (287줄)
└── data/ (런타임 생성)
    ├── input/                             # 입력 CSV 파일
    ├── warehouse/                         # Hive 외부 테이블 (Parquet)
    └── output/                            # WAU 결과 CSV
```

### 주요 파일 설명

#### `build.sbt`
**역할**: 프로젝트의 모든 메타데이터 및 의존성 정의

| 항목 | 설명 |
|------|------|
| `name` | 프로젝트 이름 |
| `version` | 버전 (JAR 파일명에 사용) |
| `scalaVersion` | Scala 2.12.18 (Spark 3.5와 호환) |
| `libraryDependencies` | 외부 라이브러리 |
| `assembly / *` | Fat JAR 생성 설정 |

---

#### `Dockerfile`
**역할**: 애플리케이션을 Docker 컨테이너로 빌드하고 실행

```dockerfile
# 2단계 빌드

# Stage 1: SBT로 Fat JAR 컴파일
FROM eclipse-temurin:11-jdk-jammy AS builder
  ├─ SBT 설치
  ├─ build.sbt + project/ 복사 (의존성 캐싱)
  ├─ src/ 복사
  └─ sbt assembly 실행 → backpacker-assembly-0.1.0.jar 생성

# Stage 2: Spark 런타임 환경
FROM apache/spark:3.5.0
  ├─ Fat JAR 복사
  ├─ 필요 디렉토리 생성 (warehouse, output)
  └─ spark-submit으로 실행
```

**왜 2단계 빌드인가?**
- Stage 1은 빌드용 (SBT, 컴파일러 포함) → 무거움
- Stage 2는 런타임만 (Spark, JVM만) → 가벼움
- 최종 이미지 크기 절약 (3GB → 1.5GB)

---

#### `docker-compose.yml`
**역할**: 로컬 개발/테스트 환경 정의

```yaml
services:
  batch-job:
    build: .                              # Dockerfile로 빌드
    container_name: spark-batch-job
    volumes:
      - ./data/input:/app/data/input      # 입력 CSV (읽기 전용)
      - ./data/warehouse:/app/data/warehouse  # Hive 테이블
      - ./output:/app/output              # 결과 CSV
    ports:
      - "4040:4040"                       # Spark UI (프로세스 모니터링)
```

---

#### `BatchJob.scala` (핵심 애플리케이션)

**클래스/메서드 구조**

```scala
object BatchJob {
  // 설정
  val INPUT_PATH = "/app/data/input"
  val SESSION_GAP_SEC = 300L  // 5분
  
  // 메인 진입점
  def main(args: Array[String]): Unit = {
    1. createHiveTable()      // Hive 외부 테이블 생성
    2. processAndWrite()      // 데이터 처리 + Parquet 저장
    3. MSCK REPAIR TABLE      // 파티션 메타데이터 동기화
    4. calculateWAU()         # WAU 계산
  }
  
  // 헬퍼 메서드
  def createHiveTable(spark)        // DDL 실행
  def processAndWrite(spark)        // CSV → Parquet 변환
  def generateSessionIds(df)        // 5분 갭 세션 생성
  def inputSchema()                 // 입력 CSV 스키마 정의
  def calculateWAU(spark)           // WAU 쿼리 실행
}
```

---

## 🏗️ 아키텍처

### 데이터 처리 흐름도

```
┌─────────────────────────────────────────────────────────────────┐
│ Stage 1: 입력 데이터 로딩                                       │
│ CSV 파일 → DataFrame 로드                                       │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Stage 2: 데이터 변환                                            │
│ • UTC → KST 타임스탬프 변환 (from_utc_timestamp)                 │
│ • event_date 파티션 컬럼 추가                                    │
│ • NULL 값 필터링                                                │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Stage 3: 세션 ID 생성 (5분 갭)                                  │
│ • user_id 기준 Window 함수 (ORDER BY event_time)                │
│ • lag: 이전 이벤트 시간 조회                                    │
│ • time_diff >= 300초 → 새 세션 시작                             │
│ • session_id = "{user_id}_{session_num}"                        │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Stage 4: 파티셔닝 및 저장                                       │
│ • 복구 장치: _SUCCESS 마커로 이미 완료된 날짜 skip              │
│ • Parquet + Snappy 압축으로 저장                                │
│ • Dynamic Partition Overwrite: 해당 파티션만 덮어씀             │
│ • LOCATION: /app/data/warehouse/user_activity/                 │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Stage 5: Hive 메타스토어 동기화                                 │
│ • MSCK REPAIR TABLE user_activity                              │
│ • 파티션 메타데이터 자동 등록                                   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Stage 6: WAU 계산                                               │
│ • user_id 기준: DISTINCT user_id / 주차별                       │
│ • session_id 기준: DISTINCT session_id / 주차별                │
│ • 결과를 CSV로 내보내기                                         │
└─────────────────────────────────────────────────────────────────┘
```

---

### 핵심 기술 개념

#### 1. **Window 함수 (세션 ID 생성)**

```scala
// Step 1: user_id 기준으로 그룹화, event_time 정렬
val userWindow = Window
  .partitionBy("user_id")
  .orderBy("event_time_unix")

// Step 2: 이전 이벤트 시간과의 차이 계산
.withColumn("prev_time", lag(col("event_time_unix"), 1).over(userWindow))
.withColumn("time_diff", col("event_time_unix") - col("prev_time"))

// Step 3: 300초 이상 차이 = 새 세션
.withColumn("is_new_session", 
  when(col("prev_time").isNull.or(col("time_diff") >= 300L), 1).otherwise(0))

// Step 4: 누적 합 = 세션 번호
.withColumn("session_num", sum(col("is_new_session")).over(userWindow))

// Step 5: 세션 ID 조합
.withColumn("session_id", concat(col("user_id"), lit("_"), col("session_num")))
```

**결과 예시**
```
user_id | event_time      | time_diff | is_new | session_num | session_id
--------|-----------------|-----------|-------|-------------|----------
100     | 10:00:00 (UTC)  | NULL      | 1     | 1           | 100_1
100     | 10:02:00        | 120 sec   | 0     | 1           | 100_1
100     | 10:10:00        | 480 sec   | 1     | 2           | 100_2  ← 5분 초과!
200     | 10:00:30        | NULL      | 1     | 1           | 200_1
```

---

#### 2. **파티셔닝 및 Parquet**

```scala
// Parquet 포맷 + Snappy 압축 + event_date 기준 파티션
.write
  .mode("overwrite")
  .option("compression", "snappy")
  .partitionBy("event_date")
  .parquet(OUTPUT_PATH)
```

**디렉토리 구조**
```
data/warehouse/user_activity/
├── event_date=2019-10-01/
│   ├── part-00000.snappy.parquet
│   ├── part-00001.snappy.parquet
│   └── _SUCCESS  ← 복구 장치: 이 파일의 존재 여부로 완료 확인
├── event_date=2019-10-02/
│   ├── part-00000.snappy.parquet
│   └── _SUCCESS
└── event_date=2019-11-01/
    ├── part-00000.snappy.parquet
    └── _SUCCESS
```

**Parquet 장점**
- **압축률**: CSV 대비 10~50% 크기 (Snappy 기준)
- **쿼리 성능**: 컬럼 지향 형식으로 필요한 컬럼만 읽음
- **스키마 보존**: 데이터타입 명시적 저장

---

#### 3. **Hive 외부 테이블**

```scala
CREATE EXTERNAL TABLE IF NOT EXISTS user_activity (
  event_time TIMESTAMP,
  event_time_kst TIMESTAMP,
  ...
  session_id STRING
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION '/app/data/warehouse/user_activity'
```

**External vs Managed Table**

| 속성 | External | Managed |
|------|----------|---------|
| 데이터 위치 | LOCATION 지정 | Hive 기본 디렉토리 |
| DROP 시 | 메타데이터만 삭제 (파일 유지) | 메타데이터 + 파일 삭제 |
| 용도 | 이미 존재하는 데이터 참조 ✅ | 새로 생성하는 데이터 |

✅ 이 프로젝트에서 External Table을 사용하는 이유:
- Spark 배치 작업이 Parquet을 직접 생성
- Hive가 단순히 이 데이터를 메타데이터로 "등록"만 함
- 향후 다른 도구(Presto, Athena 등)에서도 접근 가능

---

#### 4. **복구 장치: _SUCCESS 마커**

```scala
val marker = new Path(s"$OUTPUT_PATH/event_date=$date/_SUCCESS")
val done = fs.exists(marker)

if (!done) {
  // 아직 완료되지 않은 파티션만 저장
}
```

**왜 필요한가?**
- 대규모 배치 작업은 수시간 소요 가능
- 중간에 실패하면? → _SUCCESS 파일 없음 → 재실행 시 다시 처리
- 이미 완료된 파티션 → _SUCCESS 파일 존재 → 건너뜀

---

## 🚀 빌드 및 실행

### 전제 조건
- Docker + Docker Compose 설치
- 또는 로컬: JDK 11+, SBT 1.8+, Spark 3.5.0

### 방법 1: Docker Compose (권장)

```bash
# 1. 입력 데이터 준비
mkdir -p data/input
# 여기에 2019-Oct.csv, 2019-Nov.csv 복사

# 2. 빌드 및 실행
docker-compose up --build

# 3. 결과 확인
# output/ 디렉토리에 wau_by_user_id/, wau_by_session_id/ CSV 생성
```

### 방법 2: 로컬 실행

```bash
# 1. Fat JAR 생성
sbt assembly
# → target/scala-2.12/backpacker-assembly-0.1.0.jar 생성

# 2. Spark 제출
spark-submit \
  --class com.example.BatchJob \
  --master local[*] \
  --driver-memory 4g \
  target/scala-2.12/backpacker-assembly-0.1.0.jar

# 3. 결과 확인
# output/ 디렉토리에 결과 생성
```

---

## 📊 기술 상세 설명

### 1. Scala 타입 시스템 예시

```scala
// 컴파일 타임에 타입 체크
val df: DataFrame = spark.read.csv(...)
val filtered: DataFrame = df.filter($"price" > 100)  // 컴파일 성공

// 이것은 컴파일 에러 발생
val invalid: Int = df  // Type mismatch
```

---

### 2. SBT 의존성 해석

```scala
"org.apache.spark" %% "spark-sql" % "3.5.0" % "provided"
      ↓              ↓                ↓        ↓
   Org/Group    Artifact Name   Version    Scope
```

- `%%`: Scala 버전 자동 추가 (`spark-sql_2.12`)
- `%`: Java 라이브러리 (Scala 버전 미추가)
- `provided`: Spark 클러스터에 이미 설치됨 (JAR 패키징 제외)
- `compile` (기본): JAR에 포함됨

---

### 3. Fat JAR 충돌 처리

여러 라이브러리에서 같은 파일을 제공할 때 충돌 전략:

```scala
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")  => MergeStrategy.discard
    // 모든 라이브러리의 MANIFEST.MF 제거 (새 MANIFEST 생성됨)
  
  case PathList("META-INF", xs @ _*)        => MergeStrategy.discard
    // META-INF의 다른 파일들도 제거 (인증서 등)
  
  case "reference.conf"                     => MergeStrategy.concat
    // reference.conf (설정 파일)는 병합 (TypeSafe Config 사용 시)
  
  case _                                    => MergeStrategy.first
    // 기타: 첫 번째 파일만 선택
}
```

---

### 4. 타임스탬프 변환 상세

```scala
// 1. UTC 문자열 파싱 → UTC Timestamp
.withColumn("event_time",
  to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss z"))

// 2. UTC Timestamp → KST Timestamp (UTC+9)
.withColumn("event_time_kst",
  from_utc_timestamp($"event_time", "Asia/Seoul"))

// 3. KST Timestamp → 날짜 문자열 (파티션 키)
.withColumn("event_date",
  date_format($"event_time_kst", "yyyy-MM-dd"))
```

**예시 변환**
```
Input:  "2019-10-01 00:00:00 UTC"
        ↓
event_time:     2019-10-01 00:00:00 UTC
        ↓
event_time_kst: 2019-10-01 09:00:00 KST
        ↓
event_date:     "2019-10-01"  (파티션 디렉토리 이름)
```

---

### 5. WAU 계산 쿼리

```sql
-- user_id 기준 WAU
SELECT
    YEAR(event_time_kst) AS year,
    WEEKOFYEAR(event_time_kst) AS week,
    COUNT(DISTINCT user_id) AS wau_by_user_id
FROM user_activity
GROUP BY
    YEAR(event_time_kst),
    WEEKOFYEAR(event_time_kst)
ORDER BY year, week;

-- session_id 기준 WAU
SELECT
    YEAR(event_time_kst) AS year,
    WEEKOFYEAR(event_time_kst) AS week,
    COUNT(DISTINCT session_id) AS wau_by_session_id
FROM user_activity
GROUP BY
    YEAR(event_time_kst),
    WEEKOFYEAR(event_time_kst)
ORDER BY year, week;
```

**결과 CSV 예시**
```csv
year,week,wau_by_user_id
2019,40,15234
2019,41,15892
2019,42,16123
```

---

## 📈 데이터 흐름

### 입력 CSV 형식

```csv
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
2019-10-01 00:00:00 UTC,view,100,10,cat1,BrandA,99.99,123,sess-uuid-1
2019-10-01 00:02:00 UTC,view,101,10,cat1,BrandA,89.99,123,sess-uuid-1
2019-10-01 00:10:00 UTC,cart,102,10,cat1,BrandB,199.99,123,sess-uuid-2
2019-10-01 00:15:00 UTC,purchase,102,10,cat1,BrandB,199.99,123,sess-uuid-2
```

### 처리 중간 결과

```
event_time_kst      | user_id | time_diff | session_id
2019-10-01 09:00:00 | 123     | NULL      | 123_1
2019-10-01 09:02:00 | 123     | 120       | 123_1
2019-10-01 09:10:00 | 123     | 480       | 123_2  ← 5분 초과
2019-10-01 09:15:00 | 123     | 300       | 123_2
```

### 최종 저장 구조

```
/app/data/warehouse/user_activity/
├── event_date=2019-10-01/
│   ├── part-00000.snappy.parquet  (0-50만 행)
│   ├── part-00001.snappy.parquet  (50만-100만 행)
│   └── _SUCCESS
├── event_date=2019-10-02/
│   ├── part-00000.snappy.parquet
│   └── _SUCCESS
└── ...

/app/output/
├── wau_by_user_id/
│   └── part-00000.csv  (user_id 기준 WAU)
└── wau_by_session_id/
    └── part-00000.csv  (session_id 기준 WAU)
```

---

## 🔧 성능 최적화

### 1. **Shuffle Partition 튜닝**

```scala
spark.conf.set("spark.sql.shuffle.partitions", "400")
```

- 기본값: 200
- 대규모 데이터: 400~800
- Window 함수, GROUP BY 등에서 데이터 재분배 시 파티션 수

### 2. **Parquet 압축**

```scala
.option("compression", "snappy")
```

| 압축 방식 | 크기 | 속도 | 추천 |
|---------|------|------|------|
| None | 100% | 매우 빠름 | 임시 데이터 |
| **Snappy** | **~40%** | **빠름** | ✅ 균형잡힌 선택 |
| GZIP | ~20% | 느림 | 보관용 |
| LZO | ~45% | 매우 빠름 | 특수 환경 |

### 3. **OOM 방지**

```scala
// count() 제거 → 전체 데이터 메모리 적재 방지
// .cache() 제거 → 중간 결과 메모리 유지 안 함
```

---

## 🐛 트러블슈팅

### Q. "JAR failed: Class not found"

**원인**: Fat JAR 생성 실패  
**해결**:
```bash
sbt clean assembly
# 또는 assembly merge strategy 확인
```

---

### Q. "Spark SQL에서 Hive 테이블을 못 찾음"

**원인**: enableHiveSupport() 호출 안 함  
**해결**:
```scala
SparkSession.builder()
  .enableHiveSupport()  // ← 필수!
  .getOrCreate()
```

---

### Q. "시간대 변환이 틀림"

**원인**: `from_utc_timestamp` 순서 헷갈림  
**정확한 사용**:
```scala
from_utc_timestamp(col_in_utc, "Asia/Seoul")  // UTC → KST
to_utc_timestamp(col_in_kst, "Asia/Seoul")    // KST → UTC
```

---

## 📚 참고 자료

- [Apache Spark 공식 문서](https://spark.apache.org/docs/latest/)
- [Scala 공식 가이드](https://docs.scala-lang.org/)
- [SBT 완벽 가이드](https://www.scala-sbt.org/1.x/docs/)
- [Hive 파티셔닝](https://cwiki.apache.org/confluence/display/Hive/Partitioned+tables)
- [Parquet 포맷](https://parquet.apache.org/)

---

## 📝 라이선스

MIT License - 자유롭게 사용, 수정, 배포 가능합니다.

---

**작성일**: 2026-05-20  
**프로젝트명**: Backpacker - E-Commerce User Activity Batch Processing
