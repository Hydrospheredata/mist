import AssemblyKeys._

assemblySettings

name := "mist"

version := "0.0.1"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots")
)

resolvers += Resolver.url("artifactory", url("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)

val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("1.5.2")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.json4s" %% "json4s-native" % "3.2.11",
  "org.json4s" %% "json4s-jackson" % "3.2.11",
  "com.typesafe" % "config" % "1.3.0",
  "com.typesafe.akka" %% "akka-actor" % "2.4.1",
  "com.typesafe.akka" %% "akka-http-core-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-experimental" % "2.0.1",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.0.1",
  "net.sigusr" %% "scala-mqtt-client" % "0.6.0",
  "com.github.fge" % "json-schema-validator" % "2.2.6",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test"
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
