import AssemblyKeys._
import sbt.Keys._

assemblySettings

name := "mist"

organization := "io.hydrosphere"

version := "0.3.0"

val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, 1.6.2]")

scalaVersion := {
  sparkVersion match {
    case versionRegex(major, minor) if major.toInt == 1 && List(4, 5, 6).contains(minor.toInt) => "2.10.6"
    case versionRegex(major, minor) if major.toInt > 1 => "2.11.8"
    case _ => "2.10.6"
  }
}

crossScalaVersions := Seq("2.10.6", "2.11.8")

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)
resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

libraryDependencies <++= scalaVersion(akkaDependencies)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.2.10",
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "com.typesafe" % "config" % "1.3.0",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.1",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.typesafe.akka" %% "akka-testkit" % "2.3.12" % "test",
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test",
  "org.mapdb" % "mapdb" % "3.0.0-M6",
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.2",

  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "org.slf4j" % "slf4j-api" % "1.7.1",
 // "org.slf4j" % "log4j-over-slf4j" % "1.7.1",  // for any java classes looking for this
  "ch.qos.logback" % "logback-classic" % "1.0.3"
)

dependencyOverrides += "com.typesafe" % "config" % "1.3.0"

def akkaDependencies(scalaVersion: String) = {
  val Old = """2\.10\..""".r

  scalaVersion match {
    case Old() => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.3.15",
      "com.typesafe.akka" %% "akka-cluster" % "2.3.15"
    )
    case _ => Seq(
      "com.typesafe.akka" %% "akka-actor" % "2.4.7",
      "com.typesafe.akka" %% "akka-cluster" % "2.4.7"
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
