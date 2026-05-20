package com.example

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * E-Commerce User Activity Batch Job
 *
 * 처리 흐름:
 *   1. CSV 읽기 (2019-Oct.csv, 2019-Nov.csv)
 *   2. UTC → KST 변환 + event_date 파티션 컬럼 생성
 *   3. user_id 기준 5분 갭 세션 ID 생성 (Window 함수)
 *   4. Parquet + Snappy 로 External Hive Table 경로에 저장
 *   5. MSCK REPAIR TABLE 로 파티션 등록
 *   6. WAU 계산 (user_id / session_id 기준)
 *
 * 복구 장치:
 *   - 파티션별 _SUCCESS 마커 확인 → 이미 완료된 날짜 skip
 *   - overwrite 모드로 부분 실패 파티션 재처리 가능
 *   - Dynamic Partition Overwrite 로 다른 파티션 보존
 */
object BatchJob {

  // ── 경로 설정 ─────────────────────────────────────────────────────────────────
  val INPUT_PATH     = "/app/data/input"
  val WAREHOUSE_PATH = "/app/data/warehouse"
  val OUTPUT_PATH    = s"$WAREHOUSE_PATH/user_activity"
  val OUTPUT_DIR     = "/app/output"

  // 세션 종료 기준: 5분 (300초)
  val SESSION_GAP_SEC = 300L

  // ── Main ──────────────────────────────────────────────────────────────────────
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("EcommerceUserActivityBatch")
      .config("spark.sql.warehouse.dir", WAREHOUSE_PATH)
      // Dynamic Partition Overwrite: 다른 파티션은 건드리지 않고 해당 파티션만 덮어씀
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      // Snappy 압축
      .config("spark.sql.parquet.compression.codec", "snappy")
      // Hive 내장 메타스토어 활성화 (Derby 임베디드)
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("=" * 65)
    println("  E-Commerce User Activity Batch 시작")
    println("=" * 65)

    // 1. Hive External Table 생성
    createHiveTable(spark)

    // 2. 데이터 처리 + 파티션 저장
    processAndWrite(spark)

    // 3. Hive 파티션 메타데이터 동기화
    println("[INFO] MSCK REPAIR TABLE 실행 중...")
    spark.sql("MSCK REPAIR TABLE user_activity")
    println("[INFO] 파티션 등록 완료")

    // 4. WAU 계산
    calculateWAU(spark)

    println("=" * 65)
    println("  배치 처리 완료!")
    println("=" * 65)

    spark.stop()
  }

  // ── 1. Hive External Table DDL ────────────────────────────────────────────────
  def createHiveTable(spark: SparkSession): Unit = {
    spark.sql(s"""
      CREATE EXTERNAL TABLE IF NOT EXISTS user_activity (
        event_time      TIMESTAMP  COMMENT 'UTC 원본 이벤트 시각',
        event_time_kst  TIMESTAMP  COMMENT 'KST(UTC+9) 변환 시각',
        event_type      STRING     COMMENT 'view / cart / purchase',
        product_id      BIGINT     COMMENT '상품 ID',
        category_id     BIGINT     COMMENT '카테고리 ID',
        category_code   STRING     COMMENT '카테고리 코드',
        brand           STRING     COMMENT '브랜드',
        price           DOUBLE     COMMENT '가격',
        user_id         BIGINT     COMMENT '사용자 ID',
        user_session    STRING     COMMENT '원본 세션 UUID',
        session_id      STRING     COMMENT '5분 갭 기반 생성 세션 ID'
      )
      PARTITIONED BY (event_date STRING COMMENT 'KST 기준 날짜 yyyy-MM-dd')
      STORED AS PARQUET
      LOCATION '$OUTPUT_PATH'
      TBLPROPERTIES (
        'parquet.compression' = 'SNAPPY',
        'created.by'          = 'EcommerceUserActivityBatch'
      )
    """)
    println("[INFO] Hive External Table 'user_activity' 준비 완료")
  }

  // ── 2. 데이터 처리 + 파티션 저장 ──────────────────────────────────────────────
  def processAndWrite(spark: SparkSession): Unit = {
    import spark.implicits._

    // 셔플 파티션 수 조정 (기본 200 → 대용량 데이터에 맞게)
    spark.conf.set("spark.sql.shuffle.partitions", "400")

    println("[INFO] CSV 읽기 시작...")
    val rawDf = spark.read
      .option("header", "true")
      .option("nullValue", "")
      .schema(inputSchema())
      .csv(s"$INPUT_PATH/*.csv")
      // .limit(50000)

    println("[INFO] 데이터 개수 : " + rawDf.count())

    // ── UTC → KST 변환, event_date 파티션 컬럼 추가
    val kstDf = rawDf
      .withColumn("event_time",
        to_timestamp($"event_time", "yyyy-MM-dd HH:mm:ss z"))
      .withColumn("event_time_kst",
        from_utc_timestamp($"event_time", "Asia/Seoul"))
      .withColumn("event_date",
        date_format($"event_time_kst", "yyyy-MM-dd"))
      .filter($"event_time".isNotNull && $"user_id".isNotNull)
      // .drop("event_time") 은 삭제합니다 (event_time 컬럼이 이후에도 필요하기 때문)

    // ── 세션 ID 생성 (전체 데이터 기준 — 날짜 경계 무관)
    // count()/cache() 제거 → 1억 건 OOM 방지
    println("[INFO] 세션 ID 생성 중...")
    val sessionDf = generateSessionIds(kstDf)

    // ── 복구 장치: _SUCCESS 마커 기반 파티션 skip
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // event_date 목록 수집 (distinct + collect — 날짜 수는 적음)
    val allDates = sessionDf
      .select("event_date")
      .distinct()
      .collect()
      .map(_.getString(0))
      .sorted

    println(s"[INFO] 처리 대상 날짜 (${allDates.length}개): ${allDates.mkString(", ")}")

    val pendingDates = allDates.filterNot { date =>
      val marker = new Path(s"$OUTPUT_PATH/event_date=$date/_SUCCESS")
      val done   = fs.exists(marker)
      if (done) println(s"[SKIP] $date — 이미 완료된 파티션")
      done
    }

    if (pendingDates.isEmpty) {
      println("[INFO] 모든 파티션이 이미 처리됨 — 저장 단계 건너뜀")
    } else {
      println(s"[INFO] 저장 대상 (${pendingDates.length}개): ${pendingDates.mkString(", ")}")

      val outputCols = Seq(
        "event_time", "event_time_kst", "event_type",
        "product_id", "category_id", "category_code",
        "brand", "price", "user_id", "user_session",
        "session_id", "event_date"
      )

      println("[INFO] ========================================================")
      println("[INFO] 대규모 Window 연산(세션 정렬) 및 Parquet 저장을 시작합니다.")
      println("[INFO] (1억 건 처리 시 로컬 환경에서 수십 분 이상 소요될 수 있습니다.)")
      println("[INFO] 진행 상황은 http://localhost:4040 (Spark UI)에서 확인하세요.")
      println("[INFO] ========================================================")

      sessionDf
        .filter($"event_date".isin(pendingDates: _*))
        .select(outputCols.map(col): _*)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("event_date")
        .parquet(OUTPUT_PATH)

      println(s"[INFO] Parquet+Snappy 저장 완료 → $OUTPUT_PATH")
    }
  }

  // ── 세션 ID 생성 (5분 갭 기반 Window 함수) ────────────────────────────────────
  // 알고리즘:
  //   1. user_id 별로 event_time 오름차순 정렬
  //   2. 이전 이벤트와 시간 차이(초) 계산 (lag)
  //   3. 차이 >= 300초 이거나 첫 이벤트이면 새 세션 경계(is_new_session=1)
  //   4. 누적 합 = user 내 세션 번호
  //   5. session_id = "{user_id}_{session_num}"
  def generateSessionIds(df: DataFrame): DataFrame = {
    val userWindow = Window
      .partitionBy("user_id")
      .orderBy("event_time_unix")

    df
      .withColumn("event_time_unix", unix_timestamp(col("event_time")))
      .withColumn("prev_time",
        lag(col("event_time_unix"), 1).over(userWindow))
      .withColumn("time_diff",
        col("event_time_unix") - col("prev_time"))
      .withColumn("is_new_session",
        when(col("prev_time").isNull
          .or(col("time_diff") >= SESSION_GAP_SEC), 1L
        ).otherwise(0L))
      .withColumn("session_num",
        sum(col("is_new_session")).over(userWindow))
      .withColumn("session_id",
        concat(col("user_id"), lit("_"), col("session_num")))
      .drop("event_time_unix", "prev_time", "time_diff",
            "is_new_session", "session_num")
  }

  // ── 입력 CSV 스키마 ───────────────────────────────────────────────────────────
  def inputSchema(): StructType = StructType(Seq(
    StructField("event_time", StringType,  nullable = true),
    StructField("event_type",     StringType,  nullable = true),
    StructField("product_id",     LongType,    nullable = true),
    StructField("category_id",    LongType,    nullable = true),
    StructField("category_code",  StringType,  nullable = true),
    StructField("brand",          StringType,  nullable = true),
    StructField("price",          DoubleType,  nullable = true),
    StructField("user_id",        LongType,    nullable = true),
    StructField("user_session",   StringType,  nullable = true)
  ))

  // ── 4. WAU 계산 ───────────────────────────────────────────────────────────────
  def calculateWAU(spark: SparkSession): Unit = {
    println("\n" + "=" * 65)
    println("  WAU (Weekly Active Users) 계산")
    println("=" * 65)

    // ── user_id 기준 WAU ──────────────────────────────────────────────────
    val wauUserQuery =
      """
        |SELECT
        |    YEAR(event_time_kst)          AS year,
        |    WEEKOFYEAR(event_time_kst)    AS week,
        |    COUNT(DISTINCT user_id)       AS wau_by_user_id
        |FROM user_activity
        |GROUP BY
        |    YEAR(event_time_kst),
        |    WEEKOFYEAR(event_time_kst)
        |ORDER BY year, week
        |""".stripMargin

    println("\n[6-a] user_id 기준 WAU 쿼리:")
    println(wauUserQuery)
    val wauByUser = spark.sql(wauUserQuery)
    wauByUser.show(100, truncate = false)

    // ── session_id 기준 WAU ───────────────────────────────────────────────
    val wauSessionQuery =
      """
        |SELECT
        |    YEAR(event_time_kst)          AS year,
        |    WEEKOFYEAR(event_time_kst)    AS week,
        |    COUNT(DISTINCT session_id)    AS wau_by_session_id
        |FROM user_activity
        |GROUP BY
        |    YEAR(event_time_kst),
        |    WEEKOFYEAR(event_time_kst)
        |ORDER BY year, week
        |""".stripMargin

    println("[6-b] session_id 기준 WAU 쿼리:")
    println(wauSessionQuery)
    val wauBySession = spark.sql(wauSessionQuery)
    wauBySession.show(100, truncate = false)

    // ── 결과 CSV 저장 ──────────────────────────────────────────────────────────
    wauByUser
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"$OUTPUT_DIR/wau_by_user_id")

    wauBySession
      .write.mode("overwrite")
      .option("header", "true")
      .csv(s"$OUTPUT_DIR/wau_by_session_id")

    println(s"\n[INFO] WAU 결과 저장 완료 → $OUTPUT_DIR/")
  }
}
