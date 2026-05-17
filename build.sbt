name := "backpacker"
version := "0.1.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"  // Hive External Table 지원
)

// fat JAR 설정
assembly / mainClass        := Some("com.example.BatchJob")
assembly / assemblyJarName  := "backpacker-assembly-0.1.0.jar"

// 충돌 파일 처리 전략
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "MANIFEST.MF")          => MergeStrategy.discard
  case PathList("META-INF", xs @ _*)                => MergeStrategy.discard
  case PathList("module-info.class")                => MergeStrategy.discard
  case "reference.conf"                             => MergeStrategy.concat
  case _                                            => MergeStrategy.first
}
