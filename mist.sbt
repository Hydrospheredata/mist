import sbt.Keys._
import StageDist._
import complete.DefaultParsers._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.AssemblyOption

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns),
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "maxaf-releases" at s"http://repo.bumnetworks.com/releases/"
)

lazy val is2_10: SettingKey[Boolean] = settingKey[Boolean]("Scala version")
lazy val sparkVersion: SettingKey[String] = settingKey[String]("Spark version")
lazy val sparkLocal: TaskKey[File] = taskKey[File]("Download spark distr")
lazy val mistRun: InputKey[Unit] = inputKey[Unit]("Run mist locally")

lazy val versionRegex = "(\\d+)\\.(\\d+).*".r

lazy val commonSettings = Seq(
  organization := "io.hydrosphere",

  sparkVersion := util.Properties.propOrElse("sparkVersion", "1.5.2"),
  scalaVersion := (
    sparkVersion.value match {
      case versionRegex("1", minor) => "2.10.6"
      case _ => "2.11.8"
    }),

  is2_10 := scalaVersion.value.startsWith("2.10"),
  version := "0.13.3"
)

lazy val mistLib = project.in(file("mist-lib"))
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(
    scalacOptions ++= commonScalacOptions,
    scalacOptions += "-Xlog-implicits",
    name := s"mist-lib-spark${sparkVersion.value}",
    libraryDependencies ++= {
      if (is2_10.value ) {
        Seq(
          "com.chuusai" %% "shapeless" % "2.3.2",
          compilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
        )
      } else {
        Seq("com.chuusai" %% "shapeless" % "2.3.2")
      }
    },
    //sourceGenerators in Compile += (sourceManaged in Compile).map(Boilerplate.gen).taskValue,
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    libraryDependencies ++= Seq(
      Library.Akka.streams,
      Library.slf4j % "test",
      Library.slf4jLog4j % "test",
      Library.scalaTest % "test"
    )
  )

lazy val core = project.in(file("mist/core"))
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-core",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    libraryDependencies ++= Seq(
      Library.slf4j,
      Library.mockito % "test", Library.scalaTest % "test"
    )
  )

lazy val master = project.in(file("mist/master"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(commonAssemblySettings: _*)
  .enablePlugins(BuildInfoPlugin)
  .settings(
    name := "mist-master",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies ++= Library.Akka.base(scalaVersion.value) ++ Library.Akka.http,
    libraryDependencies += Library.Akka.testKit(scalaVersion.value),
    libraryDependencies ++= Seq(
      Library.slf4j, Library.typesafeConfig, Library.scopt,
      Library.slick, Library.h2, Library.flyway,
      Library.chill,
      Library.kafka, Library.pahoMqtt,

      Library.Akka.httpSprayJson, Library.Akka.httpTestKit % "test",
      Library.cats,

      Library.hadoopCommon, Library.commonsCodec, Library.scalajHttp,

      Library.scalaTest % "test",
      Library.mockito % "test"
    ),
    libraryDependencies ++= Library.hadoopMinicluster,
    libraryDependencies ++= {
      val is2_10 = """2\.10\..""".r
      scalaVersion.value match {
        case is2_10() => Seq("org.apache.spark" %% "spark-core" % sparkVersion.value % "provided")
        case _ => Seq()
      }
    }
  ).settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sparkVersion),
    buildInfoPackage := "io.hydrosphere.mist"
  )

lazy val worker = project.in(file("mist/worker"))
  .dependsOn(core % "compile->compile;test->test")
  .settings(commonSettings: _*)
  .settings(commonAssemblySettings: _*)
  .settings(
    name := "mist-worker",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies ++= Library.Akka.base(scalaVersion.value) ++ Library.Akka.http,
    libraryDependencies += Library.Akka.testKit(scalaVersion.value),
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    libraryDependencies ++= Seq(
      Library.scopt,
      Library.scalaTest % "test"
    ),
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("scopt.**" -> "shaded.@0").inAll
    )
  )

lazy val currentExamples = util.Properties.propOrElse("sparkVersion", "1.5.2") match {
  case versionRegex("1", minor) => examplesSpark1
  case _ => examplesSpark2
}

lazy val root = project.in(file("."))
  .aggregate(mistLib, core, master, worker)
  .dependsOn(master)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings: _*)
  .settings(Ui.settings: _*)
  .settings(StageDist.settings: _*)
  .settings(
    name := "mist",

    stageDirectory := target.value / s"mist-${version.value}-${sparkVersion.value}",
    stageActions := {
      val sparkMajor = if (sparkVersion.value.startsWith("1.")) "1" else "2"
      val routes = {
        CpFile(s"configs/router-examples-spark$sparkMajor.conf")
          .as("router.conf")
          .to("configs")
      }
      Seq(
        CpFile("bin"),
        MkDir("configs"),
        CpFile("configs/default.conf").to("configs"),
        CpFile("configs/logging").to("configs"),
        routes,
        CpFile("examples/examples-python").as("examples-python"),
        CpFile(assembly.in(master, assembly).value).as("mist-master.jar"),
        CpFile(assembly.in(worker, assembly).value).as("mist-worker.jar"),
        CpFile(sbt.Keys.`package`.in(currentExamples, Compile).value)
          .as(s"mist-examples-spark$sparkMajor.jar"),
        CpFile(Ui.ui.value).as("ui")
      )
    },
    stageActions in basicStage +=
      CpFile("configs/default.conf").to("configs"),
    stageActions in dockerStage +=
      CpFile("configs/docker.conf").as("default.conf").to("configs")
  ).settings(
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
      val sparkHome = sparkLocal.value.getAbsolutePath

      val taskArgs = spaceDelimited("<arg>").parsed
      val uiEnvs = {
        val uiPath =
          taskArgs.grouped(2)
            .find(parts => parts.size > 1 && parts.head == "--ui-dir")
            .map(_.last)

        uiPath.fold(Seq.empty[(String, String)])(p => Seq("MIST_UI_DIR" -> p))
      }
      val extraEnv = Seq("SPARK_HOME" -> sparkHome) ++ uiEnvs
      val home = basicStage.value

      val args = Seq("bin/mist-master", "start", "--debug", "true")
      val ps = Process(args, Some(home), extraEnv: _*)
      log.info(s"Running mist $ps with env $extraEnv")

      ps.!<(StdOutLogger)
    }
  ).settings(
    imageNames in docker := Seq(
      ImageName(s"hydrosphere/mist:${version.value}-${sparkVersion.value}")
    ),
    dockerfile in docker := {
      val localSpark = sparkLocal.value
      val mistHome = "/usr/share/mist"
      val distr = dockerStage.value

      new Dockerfile {
        from("anapsix/alpine-java:8")
        env("SPARK_VERSION", sparkVersion.value)
        env("SPARK_HOME", "/usr/share/spark")
        env("MIST_HOME", mistHome)

        copy(localSpark, "/usr/share/spark")
        copy(distr, mistHome)

        copy(file("docker-entrypoint.sh"), "/")
        run("chmod", "+x", "/docker-entrypoint.sh")

        run("apk", "update")
        run("apk", "add", "python", "curl", "jq", "coreutils")

        workDir(mistHome)
        entryPoint("/docker-entrypoint.sh")
      }
    })
  .configs(IntegrationTest)
  .settings(Defaults.itSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "io.spray" %% "spray-json" % "1.3.2" % "it",
      "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0" % "it",
      "org.scalaj" %% "scalaj-http" % "2.3.0" % "it",
      "org.scalatest" %% "scalatest" % "3.0.1" % "it",
      "org.testcontainers" % "testcontainers" % "1.2.1" % "it",
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % "it"
    ),
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    scalaSource in IntegrationTest := baseDirectory.value / "mist-tests" / "scala",
    resourceDirectory in IntegrationTest := baseDirectory.value / "mist-tests" / "resources",
    parallelExecution in IntegrationTest := false,
    fork in(IntegrationTest, test) := true,
    fork in(IntegrationTest, testOnly) := true,
    envVars in IntegrationTest ++= Map(
      "SPARK_HOME" -> s"${sparkLocal.value}",
      "MIST_HOME" -> s"${basicStage.value}"
    ),
    javaOptions in(IntegrationTest, test) ++= {
      val mistHome = basicStage.value
      Seq(
        s"-DsparkHome=${sparkLocal.value}",
        s"-DmistHome=$mistHome",
        s"-DsparkVersion=${sparkVersion.value}",
        "-Xmx512m"
      )
    },
    javaOptions in(IntegrationTest, testOnly) ++= {
      val mistHome = basicStage.value
      Seq(
        s"-DsparkHome=${sparkLocal.value}",
        s"-DmistHome=$mistHome",
        s"-DsparkVersion=${sparkVersion.value}",
        "-Xmx512m"
      )
    }

  )

addCommandAlias("testAll", ";test;it:test")

lazy val examplesSpark1 = project.in(file("examples/examples-spark1"))
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark1",
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    autoScalaLibrary := false
  )

lazy val examplesSpark2 = project.in(file("examples/examples-spark2"))
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples-spark2",
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    // examplesspark2 works only for 2.11
    libraryDependencies ++= {
      val is2_11 = """2\.11\..""".r
      scalaVersion.value match {
        case is2_11() => Seq("io.hydrosphere" %% "spark-ml-serving" % "0.1.2")
        case _ => Seq.empty
      }
    },
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assembledMappings in assembly := {
      // hack - there is no options how to exclude all dependecies that comes
      // from `.dependsOn(mistLib)`, setup mappings manually - only jobs + spark-ml-serving
      def isServingLib(f: File): Boolean = {
        val name = f.getName
        name.startsWith("spark-ml-serving_2.11")
      }
      def isProjectClasses(f: File): Boolean = f.getAbsolutePath.endsWith(baseDirectory.value + "/target/scala-2.11/classes")

      val x = (fullClasspath in assembly).value
      val filtered = x.seq.filter(v => {
        val file = v.data
        isServingLib(file) || isProjectClasses(file)
      })
      val s = (streams in assembly).value
      Assembly.assembleMappings(filtered, Nil, (assemblyOption in assembly).value, s.log)
    },
    sbt.Keys.`package` in Compile := (assembly in assembly).value

  )


lazy val commonAssemblySettings = Seq(
  mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
    case PathList("org", "apache", xs@_*) => MergeStrategy.first
    case PathList("org", "jboss", xs@_*) => MergeStrategy.first
    case "about.html" => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case PathList("org", "datanucleus", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
  },
  logLevel in assembly := Level.Error,
  test in assembly := {}
)

lazy val commonScalacOptions = Seq(
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)
