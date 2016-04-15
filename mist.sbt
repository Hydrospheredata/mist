import AssemblyKeys._

assemblySettings

name := "mist"

version := "0.0.1"

val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2,)")

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
  Resolver.sonatypeRepo("snapshots"),
  "MQTT Repository" at "https://repo.eclipse.org/content/repositories/paho-releases/"
)

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "com.typesafe" % "config" % "1.3.0",
  "com.typesafe.akka" %% "akka-actor" % "2.3.15",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.1",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "org.eclipse.paho" % "mqtt-client" % "0.4.0"
//  "org.scodec" %% "scodec-core" % "1.9.0",
//  "org.scalaz" %% "scalaz-core" % "7.1.1"
)

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}

lazy val sub = LocalProject("examples")

ScoverageSbtPlugin.ScoverageKeys.coverageMinimum := 89

ScoverageSbtPlugin.ScoverageKeys.coverageFailOnMinimum := true

parallelExecution in Test := false

test in assembly := {}
