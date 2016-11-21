import AssemblyKeys._

assemblySettings

name := "mist_examples"

version := "0.0.2"

val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[2.0.0, )")

scalaVersion := {
  sparkVersion match {
    case versionRegex(major, minor) if major.toInt == 1 && List(3, 4, 5, 6).contains(minor.toInt) => "2.10.6"
    case versionRegex(major, minor) if major.toInt > 1 => "2.11.8"
    case _ => "2.10.6"
  }
}

resolvers += "snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion
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

val exludes = new FileFilter {
  def accept(f: File) = {
    sparkVersion match {
      case versionRegex(major, minor) if major.toInt < 2 =>
        f.getPath.containsSlice("_Spark2.scala")

      case _ =>
        f.getPath.containsSlice("_Spark1.scala")
    }
  }
}

excludeFilter in Compile ~= {  _ || exludes }