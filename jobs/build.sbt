name := "jobs"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist" % "0.9.0",
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0",
  "org.apache.spark" %% "spark-mllib" % "2.0.0",
  "org.apache.spark" %% "spark-streaming" % "2.0.0",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.0.0",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.kafka" %% "kafka"  % "0.8.2.2" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
