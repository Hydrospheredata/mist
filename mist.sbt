import AssemblyKeys._
import sbt.Keys._

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns),
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "maxaf-releases" at s"http://repo.bumnetworks.com/releases/"
)

lazy val sparkVersion: SettingKey[String] = settingKey[String]("Spark version")
lazy val mistRun: TaskKey[Unit] = taskKey[Unit]("Run mist locally")

lazy val versionRegex = "(\\d+)\\.(\\d+).*".r

lazy val mlFilter = new FileFilter {
  override def accept(f: File): Boolean = f.getPath.containsSlice("mist/ml")
}

lazy val commonSettings = Seq(
  organization := "io.hydrosphere",

  sparkVersion := util.Properties.propOrElse("sparkVersion", "1.5.2"),
  scalaVersion := (
    sparkVersion.value match {
      case versionRegex("1", minor) => "2.10.6"
      case _ => "2.11.8"
  }),

  crossScalaVersions := Seq("2.10.6", "2.11.8")

)

lazy val mistLibSpark1= project.in(file("mist-lib-spark1"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist-api-spark1",
    scalaVersion := "2.10.6",
    libraryDependencies ++= sparkDependencies("1.5.2"),

    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
    )
  )

lazy val mistLibSpark2 = project.in(file("mist-lib-spark2"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist-api-spark2",
    scalaVersion := "2.11.8",
    libraryDependencies ++= sparkDependencies("2.0.0"),
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-native" % "3.2.10",
      "org.apache.parquet" % "parquet-column" % "1.7.0",
      "org.apache.parquet" % "parquet-hadoop" % "1.7.0",
      "org.apache.parquet" % "parquet-avro" % "1.7.0"
    ),

    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.8.2.0",
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
      )
  )

lazy val currentLib = util.Properties.propOrElse("sparkVersion", "1.5.2") match {
  case versionRegex("1", minor) => mistLibSpark1
  case _ => mistLibSpark2
}

lazy val currentExamples = util.Properties.propOrElse("sparkVersion", "1.5.2") match {
  case versionRegex("1", minor) => examplesSpark1
  case _ => examplesSpark2
}

lazy val mist = project.in(file("."))
  .dependsOn(currentLib)
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(commonAssemblySettings: _*)
  .settings(mistRunSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist",
    libraryDependencies ++= sparkDependencies(sparkVersion.value),
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-native" % "3.2.10",
      "org.json4s" %% "json4s-jackson" % "3.2.10",

      "com.typesafe" % "config" % "1.3.1",

      "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.4",
      "com.typesafe.akka" %% "akka-http-experimental" % "2.0.4",
      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.4",

      "com.github.fge" % "json-schema-validator" % "2.2.6",
      "org.scalactic" %% "scalactic" % "3.0.1-SNAP1" % "test",
      "org.scalatest" %% "scalatest" % "3.0.1-SNAP1" % "test",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.12" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
      "org.mapdb" % "mapdb" % "3.0.2",
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
      "org.apache.hadoop" % "hadoop-client" % "2.7.3" intransitive(),

      "org.scalaj" %% "scalaj-http" % "2.3.0",
      "org.apache.kafka" %% "kafka" % "0.10.2.0",
      "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
      "org.flywaydb" % "flyway-core" % "4.1.1"
    ),

    libraryDependencies ++= akkaDependencies(scalaVersion.value),
    dependencyOverrides += "com.typesafe" % "config" % "1.3.1",

    // create type-alises for compatibility between spark versions
    sourceGenerators in Compile <+= (sourceManaged in Compile, sparkVersion) map { (dir, version) => {
      val file = dir / "io" / "hydrosphere"/ "mist" / "api" / "package.scala"
      val libPackage = version match {
        case versionRegex("1", minor) => "io.hydrosphere.mist.lib.spark1"
        case _ => "io.hydrosphere.mist.lib.spark2"
      }
      val content = s"""package io.hydrosphere.mist
           |
           |package object api {
           |
           |  type SetupConfiguration = $libPackage.SetupConfiguration
           |  val SetupConfiguration = $libPackage.SetupConfiguration
           |
           |  type MistJob = $libPackage.MistJob
           |
           |  type MLMistJob = $libPackage.MLMistJob
           |
           |  type StreamingSupport = $libPackage.StreamingSupport
           |
           |  type SQLSupport = $libPackage.SQLSupport
           |
           |  type HiveSupport = $libPackage.HiveSupport
           |
           |}
        """.stripMargin
      IO.write(file,content)
      Seq(file)
    }},

    parallelExecution in Test := false,

    excludeFilter in Compile := (
      sparkVersion.value match {
        case versionRegex("1", minor) => "*_Spark2.scala" || mlFilter
        case _ => "*_Spark1.scala"
      }
    )
  ).settings(
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 30,
    ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true
  )

lazy val examples = project.in(file("examples"))
  .dependsOn(LocalProject("mist"))
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist_examples",
    version := "0.8.0",
    libraryDependencies ++= sparkDependencies(sparkVersion.value),

    excludeFilter in Compile := (
      sparkVersion.value match {
        case versionRegex("1", minor) => "*_Spark2.scala"
        case _ => "*_Spark1.scala"
      }
    )
  )

lazy val examplesSpark1 = project.in(file("examples-spark1"))
  .dependsOn(mistLibSpark1)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark1",
    scalaVersion := "2.10.6",
    version := "0.9.0",
    libraryDependencies ++= sparkDependencies("1.5.2")
  )

lazy val examplesSpark2 = project.in(file("examples-spark2"))
  .dependsOn(mistLibSpark2)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark2",
    scalaVersion := "2.11.8",
    version := "0.9.0",
    libraryDependencies ++= sparkDependencies("2.0.0")
  )

lazy val mistRunSettings = Seq(
  mistRun := {
    val log = streams.value.log
    val version = sparkVersion.value

    val local = file("spark_local")
    if (!local.exists())
      IO.createDirectory(local)

    val sparkDir = local / SparkLocal.distrName(version)
    if (!sparkDir.exists()) {
      log.info(s"Downloading spark $version to $sparkDir")
      SparkLocal.downloadSpark(version, local)
    }

    val jar = outputPath.in(Compile, assembly).value

    val sparkHome = sparkDir.getAbsolutePath
    val extraEnv = Seq(
      "SPARK_HOME" -> sparkDir.getAbsolutePath,
      "PYTHONPATH" -> s"${baseDirectory.value}/src/main/python:$sparkHome/python:`readlink -f $${SPARK_HOME}/python/lib/py4j*`:$${PYTHONPATH}"
    )
    val home = baseDirectory.value

    val config = if (version.startsWith("1."))
      "default_spark1.conf"
    else
      "default_spark2.conf"

    val args = Seq(
      "bin/mist", "start", "master",
        "--jar", jar.getAbsolutePath,
        "--config", s"configs/$config"
    )
    val ps = Process(args, Some(home), extraEnv: _*)
    log.info(s"Running mist $ps with env $extraEnv")

    ps.!<(StdOutLogger)
  },
  //assembly mist and package examples before run
  mistRun <<= mistRun.dependsOn(assembly),
  mistRun <<= mistRun.dependsOn(sbt.Keys.`package`.in(currentExamples, Compile))
)

def akkaDependencies(scalaVersion: String) = {
  val New = """2\.11\..""".r

  scalaVersion match {
    case New() => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.7",
      "com.typesafe.akka" %% "akka-cluster" % "2.4.7",
      "ch.qos.logback" % "logback-classic" % "1.1.7", //logback, in order to log to file
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "com.typesafe.akka" %% "akka-slf4j" % "2.4.1", // needed for logback to work
      "com.typesafe.slick" %% "slick" % "3.2.0"
    )
    case _ => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-cluster" % "2.3.15",
      "org.slf4j" % "slf4j-api" % "1.7.22",
      "ch.qos.logback" % "logback-classic" % "1.0.3",
      "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      "com.typesafe.slick" %% "slick" % "3.1.1"
    )
  }
}

def sparkDependencies(v: String) =
  Seq(
    "org.apache.spark" %% "spark-core" % v % "provided",
    "org.apache.spark" %% "spark-sql" % v % "provided",
    "org.apache.spark" %% "spark-hive" % v % "provided",
    "org.apache.spark" %% "spark-streaming" % v % "provided",
    "org.apache.spark" %% "spark-mllib" % v % "provided"
  )

lazy val commonAssemblySettings = Seq(
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case PathList("org", "datanucleus", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }},
  test in assembly := {}
)
