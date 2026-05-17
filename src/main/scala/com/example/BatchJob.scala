package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchJob {

  def main(args: Array[String]): Unit = {

    // ─── 1. SparkSession 초기화 ───────────────────────────────────────────────
    val spark = SparkSession.builder()
      .appName("Backpacking-SalesBatchJob")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("=" * 60)
    println("  Backpacking Spark 배치 처리 시작")
    println("=" * 60)

    // ─── 2. CSV 읽기 ─────────────────────────────────────────────────────────
    val inputPath = "/app/data/input/sales.csv"
    val NovPath = "/app/data/input/2019-Nov.csv"
    val OctPath = "/app/data/input/2019-Oct.csv"

    val OctDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv(OctPath)
    
    val NovDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv(NovPath)
      
    val rawDf = OctDf.union(NovDf)

    println(s"\n[INFO] 원본 데이터 로드 완료 — 총 ${rawDf.count()} 건\n")
    rawDf.printSchema()
    rawDf.show(truncate = false)

    // ─── 3. 카테고리별 매출 집계 ──────────────────────────────────────────────
    val categorySummary = rawDf
      .groupBy("category")
      .agg(
        sum("amount").cast("long").as("총_매출액"),
        count("*").as("주문_건수"),
        avg("amount").cast("long").as("평균_주문액"),
        sum("quantity").as("총_판매수량"),
        max("amount").as("최대_단건_금액")
      )
      .orderBy(desc("총_매출액"))

    println("\n[RESULT] 카테고리별 매출 분석")
    println("-" * 60)
    categorySummary.show(truncate = false)

    // ─── 4. 월별 일자별 매출 트렌드 ──────────────────────────────────────────
    val dailyTrend = rawDf
      .withColumn("date", to_date($"date", "yyyy-MM-dd"))
      .groupBy("date")
      .agg(
        sum("amount").cast("long").as("일별_매출액"),
        count("*").as("주문_건수")
      )
      .orderBy("date")

    println("[RESULT] 일별 매출 트렌드")
    println("-" * 60)
    dailyTrend.show(truncate = false)

    // ─── 5. TOP 5 상품 ────────────────────────────────────────────────────────
    val topProducts = rawDf
      .groupBy("product")
      .agg(
        sum("amount").cast("long").as("총_매출액"),
        sum("quantity").as("총_판매수량")
      )
      .orderBy(desc("총_매출액"))
      .limit(5)

    println("[RESULT] TOP 5 상품")
    println("-" * 60)
    topProducts.show(truncate = false)

    // ─── 6. 결과 저장 ─────────────────────────────────────────────────────────
    val outputBase = "/app/output"

    categorySummary
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/category_summary")

    dailyTrend
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/daily_trend")

    topProducts
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv(s"$outputBase/top_products")

    println(s"\n[INFO] 결과 저장 완료 → $outputBase/")
    println("=" * 60)
    println("  배치 처리 완료!")
    println("=" * 60)

    spark.stop()
  }
}
