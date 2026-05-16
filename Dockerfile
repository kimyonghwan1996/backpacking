# ─────────────────────────────────────────────
# Stage 1: SBT로 fat JAR 빌드
# ─────────────────────────────────────────────
FROM sbtscala/scala-sbt:eclipse-temurin-jammy-11.0.17_8_1.9.7_2.12.18 AS builder

WORKDIR /build

# 의존성 캐싱 (소스 변경 시 재다운로드 방지)
COPY build.sbt .
COPY project/ project/
RUN sbt update

# 소스 복사 후 fat JAR 생성
COPY src/ src/
RUN sbt assembly

# ─────────────────────────────────────────────
# Stage 2: Spark 환경에서 실행
# ─────────────────────────────────────────────
FROM apache/spark:3.5.0

USER root
WORKDIR /app

# fat JAR 복사
COPY --from=builder /build/target/scala-2.12/backpacking-assembly-0.1.0.jar /app/app.jar

# 입력 데이터 복사
COPY data/ /app/data/

# 출력 디렉토리 생성
RUN mkdir -p /app/output

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--class", "com.example.BatchJob", \
  "--master", "local[*]", \
  "--driver-memory", "1g", \
  "/app/app.jar"]
