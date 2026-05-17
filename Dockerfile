# ─────────────────────────────────────────────
# Stage 1: SBT로 fat JAR 빌드
# ─────────────────────────────────────────────
FROM eclipse-temurin:11-jdk-jammy AS builder

WORKDIR /build

# SBT 설치
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" \
      | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt.gpg --import && \
    chmod 644 /etc/apt/trusted.gpg.d/scalasbt.gpg && \
    apt-get update && \
    apt-get install -y sbt

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
