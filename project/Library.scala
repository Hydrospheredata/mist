import sbt._

object Library {

  val slf4j = "org.slf4j" % "slf4j-api" % "1.7.5"
  val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.7.5"
  val scopt = "com.github.scopt" %% "scopt" % "3.6.0"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1"

  val chill = "com.twitter" %% "chill" % "0.9.2"

  val h2 = "com.h2database" % "h2" % "1.4.194"
  val flyway = "org.flywaydb" % "flyway-core" % "4.1.1"

  val doobieVersion = "0.6.0"
  val doobieCore     = "org.tpolecat" %% "doobie-core"     % doobieVersion
  val doobiePostgres = "org.tpolecat" %% "doobie-postgres" % doobieVersion
  val doobieH2       = "org.tpolecat" %% "doobie-h2"       % doobieVersion
  val doobieHikari   = "org.tpolecat" %% "doobie-hikari"   % doobieVersion
  val doobieSpecs2   = "org.tpolecat" %% "doobie-specs2"   % doobieVersion

  val pahoMqtt = "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0"
  val kafka = "org.apache.kafka" %% "kafka" % "0.10.2.0" exclude("log4j", "log4j") exclude("org.slf4j","slf4j-log4j12")

  // There is `cats` dependency from doobie...
  //val cats = "org.typelevel" %% "cats" % "0.9.0"

  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  val junit = "junit" % "junit" % "4.12"
  val mockito ="org.mockito" % "mockito-all" % "1.10.19"

  val dockerJava = "com.github.docker-java" % "docker-java" % "3.0.12"

  object Akka {
    val akkaVersion = "2.5.9"
    val httpVersion = "10.0.11"

    val stream = "com.typesafe.akka" %% "akka-stream" % akkaVersion
    val http = "com.typesafe.akka" %% "akka-http" % httpVersion
    val httpSprayJson = "com.typesafe.akka" %% "akka-http-spray-json" % httpVersion
    val httpTestKit = "com.typesafe.akka" %% "akka-http-testkit" % httpVersion

    val testKit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion
    val actor = "com.typesafe.akka" %% "akka-actor" % akkaVersion
    val typed = "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion
    def base = {
      Seq(
        actor,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
      )
    }
  }

  val commonsCodec = "commons-codec" % "commons-codec" % "1.10"
  val scalajHttp = "org.scalaj" %% "scalaj-http" % "2.3.0"

  def spark(v: String) = Seq(
    "org.apache.spark" %% "spark-core" % v,
    "org.apache.spark" %% "spark-sql" % v,
    "org.apache.spark" %% "spark-hive" % v,
    "org.apache.spark" %% "spark-streaming" % v,
  )

  val jsr305 = "com.google.code.findbugs" % "jsr305" % "1.3.9"

}
