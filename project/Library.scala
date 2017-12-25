import sbt._

object Library {

  val slf4j = "org.slf4j" % "slf4j-api" % "1.7.5"
  val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.7.5"
  val scopt = "com.github.scopt" %% "scopt" % "3.6.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"

  val chill = "com.twitter" %% "chill" % "0.9.2"

  val slick = "com.typesafe.slick" %% "slick" % "3.1.1"
  val h2 = "com.h2database" % "h2" % "1.4.194"
  val flyway = "org.flywaydb" % "flyway-core" % "4.1.1"

  val pahoMqtt = "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
  val kafka = "org.apache.kafka" %% "kafka" % "0.10.2.0" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")

  val cats = "org.typelevel" %% "cats" % "0.9.0"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  val mockito ="org.mockito" % "mockito-all" % "1.10.19"

  val reflect =  "org.scala-lang" % "scala-reflect" % "2.11.8"

  object Akka {
    val akkaVersion = "2.5.8"
    val httpVersion = "10.0.11"

    val stream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
    val http = "com.typesafe.akka" %% "akka-http" % httpVersion
    val httpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % httpVersion
    val httpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % httpVersion

    val testKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
    def base = {
      Seq(
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
      )
    }
  }

  val hadoopCommon = "org.apache.hadoop" % "hadoop-common" % "2.6.4"
  val hadoopMinicluster = Seq(
    "org.apache.hadoop" % "hadoop-hdfs" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-common" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-client" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-mapreduce-client-jobclient" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-tests" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % "2.6.4" % "test" classifier "" classifier "tests",
    "org.apache.hadoop" % "hadoop-minicluster" % "2.6.4" % "test"
  ).map(_.exclude("javax.servlet", "servlet-api"))

  val commonsCodec = "commons-codec" % "commons-codec" % "1.10"
  val scalajHttp = "org.scalaj" %% "scalaj-http" % "2.3.0"

  def spark(v: String) = Seq(
    "org.apache.spark" %% "spark-core" % v,
    "org.apache.spark" %% "spark-sql" % v,
    "org.apache.spark" %% "spark-hive" % v,
    "org.apache.spark" %% "spark-streaming" % v
  )

}
