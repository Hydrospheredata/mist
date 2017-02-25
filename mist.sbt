import AssemblyKeys._
import sbt.Keys._

import scala.util.matching.Regex

assemblySettings

name := "mist"

organization := "io.hydrosphere"

val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

scalaVersion := {
  sparkVersion match {
    case versionRegex(major, minor) if major.toInt == 1 => "2.10.6"
    case versionRegex(major, minor) if major.toInt > 1 => "2.11.8"
    case _ => "2.11.8"
  }
}

crossScalaVersions := Seq("2.10.6", "2.11.8")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

libraryDependencies <++= scalaVersion(akkaDependencies)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.apache.hadoop" % "hadoop-client" % "2.7.3" intransitive(),
  "com.typesafe" % "config" % "1.3.1",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.4",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.4",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.4",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.12" % "test",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "org.scalactic" %% "scalactic" % "3.0.1-SNAP1" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1-SNAP1" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "org.mapdb" % "mapdb" % "3.0.3",
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.1.0",
  "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.1.2" excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "org.xerial" % "sqlite-jdbc" % "3.8.11.2",
  "io.getquill" %% "quill-jdbc" % "1.1.1-SNAPSHOT",
  "org.flywaydb" % "flyway-core" % "4.1.1"
)

dependencyOverrides += "com.typesafe" % "config" % "1.3.1"


def akkaDependencies(scalaVersion: String) = {
  val New = """2\.11\..""".r

  scalaVersion match {
    case New() => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.7",
      "com.typesafe.akka" %% "akka-cluster" % "2.4.7",
      "ch.qos.logback" % "logback-classic" % "1.1.7",  //logback, in order to log to file
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "com.typesafe.akka" %% "akka-slf4j" % "2.4.1"   // needed for logback to work
    )
    case _ => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-cluster" % "2.3.15",
      "org.slf4j" % "slf4j-api" % "1.7.22",
      "ch.qos.logback" % "logback-classic" % "1.0.3",
      "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"
    )
  }
}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case PathList("org", "datanucleus", xs @ _*) => MergeStrategy.discard
    case _ => MergeStrategy.first
  }
}

val exludes = new FileFilter {
  def accept(f: File): Boolean = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt < 2 =>
        f.getPath.containsSlice("_Spark2.scala") || f.getPath.containsSlice("mist/ml")

      case _ =>
        f.getPath.containsSlice("_Spark1.scala")
    }
  }
}

excludeFilter in Compile ~= {  _ || exludes }

lazy val sub = LocalProject("examples")

ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 30

ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true

parallelExecution in Test := false

test in assembly := {}

//Maven
publishMavenStyle := true

credentials += Credentials(Path.userHome / ".ivy2" / ".credentials")


publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots/")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2/")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := <url>https://github.com/Hydrospheredata/mist</url>
  <licenses>
    <license>
      <name>Apache 2.0 License</name>
      <url>https://github.com/Hydrospheredata/mist/LICENSE</url>
      <distribution>repo</distribution>
    </license>
  </licenses>
  <scm>
    <url>https://github.com/Hydrospheredata/mist.git</url>
    <connection>https://github.com/Hydrospheredata/mist.git</connection>
  </scm>
  <developers>
    <developer>
      <id>mkf-simpson</id>
      <name>Konstantin Makarychev</name>
      <url>https://github.com/mkf-simpson</url>
      <organization>Hydrosphere</organization>
      <organizationUrl>http://hydrosphere.io/</organizationUrl>
    </developer>
    <developer>
      <id>leonid133</id>
      <name>Leonid Blokhin</name>
      <url>https://github.com/leonid133</url>
      <organization>Hydrosphere</organization>
      <organizationUrl>http://hydrosphere.io/</organizationUrl>
    </developer>
  </developers>
