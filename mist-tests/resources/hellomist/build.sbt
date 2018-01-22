scalaVersion := "2.11.8"
name := "hello-mist"
version := "0.0.1"

val sparkVersion = "{{SPARK_VERSION}}"

libraryDependencies ++= Seq(
  "io.hydrosphere" %% "mist-lib" % "{{MIST_VERSION}}",

  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
)
