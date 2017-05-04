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
lazy val sparkLocal: TaskKey[File] = taskKey[File]("Download spark distr")
lazy val mistRun: TaskKey[Unit] = taskKey[Unit]("Run mist locally")

lazy val versionRegex = "(\\d+)\\.(\\d+).*".r

lazy val mistScalaCrossCompile: Seq[String] = {
  val base=Seq("2.11.8")
  util.Properties.propOrElse("sparkVersion", "2.1.0") match {
    case versionRegex("2", minor) => base
    case _ => base :+ "2.10.6"
  }
}

lazy val commonSettings = Seq(
  organization := "io.hydrosphere",

  sparkVersion := util.Properties.propOrElse("sparkVersion", "1.5.2"),
  scalaVersion := (
    sparkVersion.value match {
      case versionRegex("1", minor) => "2.10.6"
      case _ => "2.11.8"
  }),

  crossScalaVersions := mistScalaCrossCompile,
  version := "0.11.0"

)

lazy val mistLibSpark1= project.in(file("mist-lib-spark1"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    name := "mist-lib-spark1",
    libraryDependencies ++= sparkDependencies("1.5.2"),

    libraryDependencies ++= Seq(
      "org.apache.kafka" % "kafka-clients" % "0.8.2.0" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"),
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
    )
  )

lazy val mistLibSpark2 = project.in(file("mist-lib-spark2"))
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .dependsOn(mistLibSpark1)
  .settings(
    name := "mist-lib-spark2",
    scalaVersion := "2.11.8",
    libraryDependencies ++= sparkDependencies("2.0.0"),
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-native" % "3.2.10",
      "org.apache.parquet" % "parquet-column" % "1.7.0",
      "org.apache.parquet" % "parquet-hadoop" % "1.7.0",
      "org.apache.parquet" % "parquet-avro" % "1.7.0",

      "org.scalatest" %% "scalatest" % "3.0.1" % "test",
      "org.slf4j" % "slf4j-api" % "1.7.5" % "test",
      "org.slf4j" % "slf4j-log4j12" % "1.7.5" % "test"
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

lazy val aggregatedProjects: Seq[ProjectReference] = {
  val base: Seq[ProjectReference] = Seq(mistLibSpark1, mist)

  util.Properties.propOrElse("sparkVersion", "2.1.0") match {
    case versionRegex("1", minor) => base
    case _ => base :+ (mistLibSpark2: ProjectReference)
  }
}

lazy val mist = project.in(file("."))
  .dependsOn(currentLib)
  .dependsOn(mistLibSpark1)
  .enablePlugins(DockerPlugin)
  .settings(assemblySettings)
  .settings(commonSettings: _*)
  .configs(IntegrationTest)
  .settings(Defaults.itSettings : _*)
  .settings(commonAssemblySettings: _*)
  .settings(mistRunSettings: _*)
  .settings(dockerSettings: _*)
  .settings(
    name := "mist",
    libraryDependencies ++= sparkDependencies(sparkVersion.value),
    libraryDependencies ++= Seq(
      "com.typesafe" % "config" % "1.3.1",
      "joda-time" % "joda-time" % "2.5",
      "org.slf4j" % "slf4j-api" % "1.7.5",
      "org.slf4j" % "slf4j-log4j12" % "1.7.5",

      "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.4",
      "com.typesafe.akka" %% "akka-http-experimental" % "2.0.4",

      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.4",

      "com.typesafe.akka" %% "akka-http-testkit-experimental" % "2.0.4" % "test",

      "org.scalatest" %% "scalatest" % "3.0.1" % "it,test",
      "com.typesafe.akka" %% "akka-testkit" % "2.3.12" % "test",

      "org.mockito" % "mockito-all" % "1.10.19" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
      "org.testcontainers" % "testcontainers" % "1.2.1" % "it",

      "org.mapdb" % "mapdb" % "1.0.9",
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
      "org.apache.hadoop" % "hadoop-client" % "2.6.4" intransitive(),

      "org.scalaj" %% "scalaj-http" % "2.3.0",
      "org.apache.kafka" %% "kafka" % "0.10.2.0" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12"),
      "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
      "org.flywaydb" % "flyway-core" % "4.1.1",
      "org.typelevel" %% "cats" % "0.9.0"
    ),

    libraryDependencies ++= akkaDependencies(scalaVersion.value),
    libraryDependencies ++= miniClusterDependencies,
    dependencyOverrides += "com.typesafe" % "config" % "1.3.1",

    // create type-alises for compatibility between spark versions
    /*sourceGenerators in Compile <+= (sourceManaged in Compile, sparkVersion) map { (dir, version) => {
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
           |  type ContextSupport = $libPackage.ContextSupport
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
           |  type Publisher = $libPackage.Publisher
           |
           |  type GlobalPublisher = $libPackage.GlobalPublisher
           |  val GlobalPublisher = $libPackage.GlobalPublisher
           |}
        """.stripMargin
      IO.write(file,content)
      Seq(file)
    }},*/

    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,

    fork in (Test, test) := true,
    fork in (IntegrationTest, test) := true,
    javaOptions in (IntegrationTest, test) ++= {
      val jar = outputPath.in(Compile, assembly).value
      Seq(
        s"-DsparkHome=${sparkLocal.value}",
        s"-DmistJar=$jar",
        s"-DsparkVersion=${sparkVersion.value}",
        "-Xmx512m"
      )
    },
    test in IntegrationTest <<= (test in IntegrationTest).dependsOn(assembly),
    test in IntegrationTest <<= (test in IntegrationTest).dependsOn(sbt.Keys.`package`.in(currentExamples, Compile))
  ).settings(
    ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 30,
    ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true
  )

addCommandAlias("testAll", ";mistLibSpark2/test;mist/test;mist/it:test")

lazy val examplesSpark1 = project.in(file("examples-spark1"))
  .dependsOn(mistLibSpark1)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark1",
    scalaVersion := "2.10.6",
    libraryDependencies ++= sparkDependencies("1.5.2")
  )

lazy val examplesSpark2 = project.in(file("examples-spark2"))
  .dependsOn(mistLibSpark1)
  .dependsOn(mistLibSpark2)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark2",
    scalaVersion := "2.11.8",
    libraryDependencies ++= sparkDependencies("2.0.0")
  )

lazy val mistRunSettings = Seq(
  sparkLocal := {
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
    sparkDir
  },
  mistRun := {
    val log = streams.value.log
    val jar = outputPath.in(Compile, assembly).value

    val version = sparkVersion.value
    val sparkHome = sparkLocal.value.getAbsolutePath
    val extraEnv = Seq(
      "SPARK_HOME" -> sparkHome
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

lazy val dockerSettings = Seq(
  imageNames in docker := Seq(
    ImageName(s"hydrosphere/mist:${version.value}-${sparkVersion.value}")
  ),
  dockerfile in docker := {
    val artifact = assembly.value
    val examples = packageBin.in(currentExamples, Compile).value
    val localSpark = sparkLocal.value

    val mistHome = "/usr/share/mist"

    val sparkMajor = sparkVersion.value.split('.').head

    val routerConfig = s"configs/router-examples-spark$sparkMajor.conf"
    val replacedPaths = scala.io.Source.fromFile(routerConfig).getLines()
      .map(s => {
        if (s.startsWith("jar_path"))
          s"""jar_path = "$mistHome/${examples.name}""""
        else
          s
      }).mkString("\n")

    val dockerRoutes = file("./target/docker_routes.conf")
    IO.write(dockerRoutes, replacedPaths.getBytes)

    new Dockerfile {
      from("anapsix/alpine-java:8")
      env("SPARK_VERSION", sparkVersion.value)
      env("SPARK_HOME", "/usr/share/spark")
      env("MIST_HOME", mistHome)

      copy(localSpark, "/usr/share/spark")

      run("mkdir", "-p", s"$mistHome")
      run("mkdir", "-p", s"$mistHome/configs")

      copy(file("bin"), s"$mistHome/bin")

      copy(file("configs/docker.conf"), s"$mistHome/configs/docker.conf")
      copy(dockerRoutes, s"$mistHome/configs/router-examples.conf")

      add(artifact, s"$mistHome/mist-assembly.jar")
      add(examples, s"$mistHome/${examples.name}")

      copy(file("examples-python"), mistHome + "/examples-python")

      copy(file("docker-entrypoint.sh"), "/")
      run("chmod", "+x", "/docker-entrypoint.sh")

      run("apk", "update")
      run("apk", "add", "python", "curl", "jq", "coreutils")

      expose(2003)
      workDir(mistHome)
      entryPoint("/docker-entrypoint.sh")
    }
  }
)

def akkaDependencies(scalaVersion: String) = {
  val New = """2\.11\..""".r

  scalaVersion match {
    case New() => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.7",
      "com.typesafe.akka" %% "akka-cluster" % "2.4.7",
      "com.typesafe.akka" %% "akka-slf4j" % "2.4.1", // needed for logback to work
      "com.typesafe.slick" %% "slick" % "3.2.0"
    )
    case _ => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-cluster" % "2.3.15",
      "com.typesafe.akka" %% "akka-slf4j" % "2.3.15", // needed for logback to work
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

lazy val miniClusterDependencies =
  Seq(
    "org.apache.hadoop" % "hadoop-hdfs" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-common" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-client" % "2.6.4" % "test" classifier "" classifier "tests" ,
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster" % "2.6.4" % "test"
  ).map(_.exclude("javax.servlet", "servlet-api"))

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
