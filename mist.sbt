import AssemblyKeys._
import sbt.Keys._

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns),
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
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

lazy val mistApiSpark1= project.in(file("mist-api-spark1"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist-api-spark1",
    scalaVersion := "2.10.6",
    libraryDependencies ++= sparkDependencies("1.5.2")
  )

lazy val mistApiSpark2 = project.in(file("mist-api-spark2"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist-api-spark2",
    scalaVersion := "2.11.8",
    libraryDependencies ++= sparkDependencies("2.0.0")
  )

lazy val currentApi = util.Properties.propOrElse("sparkVersion", "1.5.2") match {
  case versionRegex("1", minor) => mistApiSpark1
  case _ => mistApiSpark2
}

lazy val mist = project.in(file("."))
  .dependsOn(currentApi)
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

      "org.scalaj" %% "scalaj-http" % "2.3.0"
    ),

    libraryDependencies ++= akkaDependencies(scalaVersion.value),
    dependencyOverrides += "com.typesafe" % "config" % "1.3.1",

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
           |  type ContextWrapper = $libPackage.ContextWrapper
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
    val extraEnv = "SPARK_HOME" -> sparkDir.getAbsolutePath
    val home = baseDirectory.value
    val ps = Process(Seq("bin/mist", "start", "master", "--jar", jar.getAbsolutePath), Some(home), extraEnv)
    log.info(s"Running mist $ps with env $extraEnv")

    ps.!<(StdOutLogger)
  },
  //assembly mist and package examples before run
  mistRun <<= mistRun.dependsOn(assembly),
  mistRun <<= mistRun.dependsOn(sbt.Keys.`package`.in(examples, Compile))
)

def akkaDependencies(scalaVersion: String) = {
  val Old = """2\.10\..""".r

  scalaVersion match {
    case Old() => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-cluster" % "2.3.15",
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "ch.qos.logback" % "logback-classic" % "1.0.3",
      "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
    )
    case _ => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.7",
      "com.typesafe.akka" %% "akka-cluster" % "2.4.7",
      "ch.qos.logback" % "logback-classic" % "1.1.7",  //logback, in order to log to file
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
      "com.typesafe.akka" %% "akka-slf4j" % "2.4.1"   // needed for logback to work
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
