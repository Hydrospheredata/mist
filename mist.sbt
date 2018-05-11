import sbt.Keys._
import StageDist._
import complete.DefaultParsers._
import microsites.ConfigYml
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.AssemblyOption

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns),
  "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
  "maxaf-releases" at s"http://repo.bumnetworks.com/releases/"
)

lazy val sparkVersion: SettingKey[String] = settingKey[String]("Spark version")
lazy val sparkLocal: TaskKey[File] = taskKey[File]("Download spark distr")
lazy val mistRun: InputKey[Unit] = inputKey[Unit]("Run mist locally")

lazy val versionRegex = "(\\d+)\\.(\\d+).*".r

lazy val commonSettings = Seq(
  organization := "io.hydrosphere",

  sparkVersion := sys.props.getOrElse("sparkVersion", "2.3.0"),
  scalaVersion :=  "2.11.8",
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  parallelExecution in Test := false,
  version := "1.0.0-RC15"
)

lazy val mistLib = project.in(file("mist-lib"))
  .settings(commonSettings: _*)
  .settings(PublishSettings.settings: _*)
  .settings(PyProject.settings: _*)
  .settings(
    PyProject.pyName := "mistpy",
    PyProject.pyDir := baseDirectory.value / "src" / "main" / "python"
  )
  .settings(
    scalacOptions ++= commonScalacOptions,
    name := "mist-lib",
    sourceGenerators in Compile += (sourceManaged in Compile).map(dir => Boilerplate.gen(dir)).taskValue,
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    libraryDependencies ++= Seq(
      "io.hydrosphere" %% "shadedshapeless" % "2.3.0",
      Library.slf4j % "test",
      Library.slf4jLog4j % "test",
      Library.scalaTest % "test"
    ),
    parallelExecution in Test := false,
    test in Test := Def.sequential(test in Test, PyProject.pyTest in Test).value
  )

lazy val core = project.in(file("mist/core"))
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-core",
    scalacOptions ++= commonScalacOptions,
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "runtime"),
    libraryDependencies ++= Seq(
      Library.Akka.actor,
      Library.slf4j,
      Library.reflect,
      Library.Akka.testKit % "test",
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
    libraryDependencies ++= Library.Akka.base,
    libraryDependencies ++= Seq(
      Library.slf4jLog4j, Library.typesafeConfig, Library.scopt,
      Library.slick, Library.h2, Library.flyway,
      Library.chill,
      Library.kafka, Library.pahoMqtt,

      Library.Akka.testKit % "test",
      Library.Akka.http, Library.Akka.httpSprayJson, Library.Akka.httpTestKit % "test",
      Library.cats,

      Library.dockerJava,

      Library.commonsCodec, Library.scalajHttp,
      Library.jsr305 % "provided",

      Library.scalaTest % "test",
      Library.mockito % "test"
    )
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
    resourceGenerators in Compile += {
      Def.task {
        val resourceDir = (resourceManaged in Compile).value
        val f = (mistLib / PyProject.pySources).value
        val baseOut = resourceDir / "mistpy"
        f.listFiles().toSeq.map(r => {
          val target = baseOut / r.name
          IO.write(target, IO.read(r))
          target
        })
      }
    },
    libraryDependencies ++= Library.Akka.base :+ Library.Akka.http,
    libraryDependencies += Library.Akka.testKit,
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    libraryDependencies ++= Seq(
      Library.scopt,
      Library.scalaTest % "test"
    ),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyShadeRules in assembly := Seq(
       ShadeRule.rename("scopt.**" -> "shaded.@0").inAll
    )
  )

lazy val root = project.in(file("."))
  .aggregate(mistLib, core, master, worker)
  .dependsOn(master)
  .enablePlugins(DockerPlugin)
  .settings(commonSettings: _*)
  .settings(Ui.settings: _*)
  .settings(StageDist.settings: _*)
  .settings(
    name := "mist",

    stageDirectory := target.value / s"mist-${version.value}",
    stageActions := {
      Seq(
        CpFile("bin"),
        MkDir("configs"),
        CpFile("configs/default.conf").to("configs"),
        CpFile("configs/logging").to("configs"),
        CpFile(assembly.in(master, assembly).value).as("mist-master.jar"),
        CpFile(assembly.in(worker, assembly).value).as("mist-worker.jar"),
        CpFile(Ui.ui.value).as("ui")
      )
    },
    stageActions in basicStage += {
      val configData =
        IO.read(file("configs/default.conf"))
          .replaceAll("\\$\\{version\\}", version.value)
          .replaceAll("\\$\\{sparkVersion\\}", sparkVersion.value)
      Write("configs/default.conf", configData)
    },
    stageDirectory in dockerStage := target.value / s"mist-docker-${version.value}",
    stageActions in dockerStage += {
      val configData =
        IO.read(file("configs/docker.conf"))
          .replaceAll("\\$\\{version\\}", version.value)
          .replaceAll("\\$\\{sparkVersion\\}", sparkVersion.value)

      Write("configs/default.conf", configData)
    },

    stageDirectory in runStage := target.value / s"mist-run-${version.value}",
    stageActions in runStage ++= {
      val mkJfunctions = Seq(
        ("spark-ctx-example", "SparkContextExample$"),
        ("jspark-ctx-example", "JavaSparkContextExample"),
        ("streaming-ctx-example", "StreamingExample$"),
        ("jstreaming-ctx-example", "JavaStreamingContextExample"),
        ("hive-ctx-example", "HiveContextExample$"),
        ("sql-ctx-example", "SQLContextExample$"),
        ("text-search-example", "TextSearchExample$"),
        ("less-verbose-example", "LessVerboseExample$"),
        ("pi-example", "PiExample$"),
        ("jpi-example", "JavaPiExample")
      ).map({case (name, clazz) => {
        Write(
          s"data/functions/$name.conf",
          s"""path = mist-examples.jar
             |className = "$clazz"
             |namespace = foo""".stripMargin
        )
      }}) :+ CpFile(sbt.Keys.`package`.in(examples, Compile).value)
        .as(s"mist-examples.jar")
        .to("data/artifacts")

      val mkPyfunctions = Seq(
        ("session_example.py",      "session_example"),
        ("sparkctx_example.py",     "sparkctx_example"),
        ("sqlctx_example.py",       "sqlctx_example"),
        ("streamingctx_example.py", "streamingctx_example")
      ).flatMap({case (file, clazz) => {
        val name = file.replace(".py", "_py")
        Seq(
          Write(
            s"data/functions/$name.conf",
            s"""path = $file
               |className = "$clazz"
               |namespace = foo""".stripMargin
          ),
          CpFile(s"examples/examples-python/$file").to("data/artifacts")
        )
      }})

      Seq(MkDir("data/artifacts"), MkDir("data/functions")) ++ mkJfunctions ++ mkPyfunctions
    }
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
      val taskArgs = spaceDelimited("<arg>").parsed.grouped(2).toSeq
        .flatMap(l => {if (l.size == 2) Some(l.head -> l.last) else None})
        .toMap

      val uiEnvs = taskArgs.get("--ui-dir").fold(Seq.empty[(String, String)])(p => Seq("MIST_UI_DIR" -> p))
      val sparkEnvs = {
        val spark = taskArgs.getOrElse("--spark", sparkLocal.value.getAbsolutePath)
        Seq("SPARK_HOME" -> spark)
      }

      val extraEnv = sparkEnvs ++ uiEnvs
      val home = runStage.value

      val args = Seq("bin/mist-master", "start", "--debug", "true")

      import scala.sys.process._

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

        expose(2004)

        workDir(mistHome)

        env("SPARK_VERSION", sparkVersion.value)
        env("SPARK_HOME", "/usr/share/spark")
        env("MIST_HOME", mistHome)

        entryPoint("/docker-entrypoint.sh")

        run("apk", "update")
        run("apk", "add", "python", "curl", "jq", "coreutils", "subversion-dev", "fts-dev")

        copy(localSpark, "/usr/share/spark")

        copyRaw("--from=quay.io/vektorcloud/mesos /usr/local/lib/libmesos-1.4.1.so", "/usr/local/lib/libmesos.so")
        runRaw("""echo "export MESOS_NATIVE_JAVA_LIBRARY=/usr/local/lib/libmesos.so" > $SPARK_HOME/conf/spark-env.sh""")

        copy(distr, mistHome)

        copy(file("docker-entrypoint.sh"), "/")
        run("chmod", "+x", "/docker-entrypoint.sh")
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
      "org.scala-lang" % "scala-compiler" % scalaVersion.value % "it"
    ),
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided"),
    scalaSource in IntegrationTest := baseDirectory.value / "mist-tests" / "scala",
    resourceDirectory in IntegrationTest := baseDirectory.value / "mist-tests" / "resources",
    parallelExecution in IntegrationTest := false,
    fork in IntegrationTest := true,
    envVars in IntegrationTest ++= Map(
      "SPARK_HOME" -> s"${sparkLocal.value}",
      "MIST_HOME" -> s"${basicStage.value}"
    ),
    javaOptions in IntegrationTest ++= {
      val mistHome = runStage.value
      val dockerImage = {
        docker.value
        (imageNames in docker).value.head
      }
      val examplesJar = sbt.Keys.`package`.in(examples, Compile).value
      Seq(
        s"-DexamplesJar=$examplesJar",
        s"-DimageName=$dockerImage",
        s"-DsparkHome=${sparkLocal.value}",
        s"-DmistHome=$mistHome",
        s"-DsparkVersion=${sparkVersion.value}",
        "-Xmx512m"
      )
    }
  )

addCommandAlias("testAll", ";test;it:test")

lazy val examples = project.in(file("examples/examples"))
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    name := "mist-examples",
    libraryDependencies ++= Library.spark(sparkVersion.value).map(_ % "provided")
  )

lazy val docs = project.in(file("docs"))
  .enablePlugins(MicrositesPlugin)
  .dependsOn(mistLib)
  .settings(commonSettings: _*)
  .settings(
    scalaVersion := "2.11.8",
    libraryDependencies ++= Library.spark(sparkVersion.value),
    micrositeName := "Hydropshere - Mist",
    micrositeDescription := "Serverless proxy for Spark cluster",
    micrositeAuthor := "hydrosphere.io",
    micrositeHighlightTheme := "atom-one-light",
    micrositeDocumentationUrl := "api",
    micrositeGithubOwner := "Hydrospheredata",
    micrositeGithubRepo := "mist",
    micrositeBaseUrl := "/mist-docs",
    micrositePalette := Map(
      "brand-primary" -> "#052150",
      "brand-secondary" -> "#081440",
      "brand-tertiary" -> "#052150",
      "gray-dark" -> "#48494B",
      "gray" -> "#7D7E7D",
      "gray-light" -> "#E5E6E5",
      "gray-lighter" -> "#F4F3F4",
      "white-color" -> "#FFFFFF"),
    ghpagesNoJekyll := false,
    git.remoteRepo := "git@github.com:Hydrospheredata/mist.git",
    micrositeConfigYaml := ConfigYml(
      yamlCustomProperties = Map("version" -> version.value)
    )
  )


lazy val commonAssemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
    case PathList("org", "apache", xs@_*) => MergeStrategy.first
    case PathList("org", "jboss", xs@_*) => MergeStrategy.first
    case "about.html" => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case PathList("org", "datanucleus", xs@_*) => MergeStrategy.discard
    case _ => MergeStrategy.first
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
