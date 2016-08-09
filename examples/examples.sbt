import AssemblyKeys._

assemblySettings

name := "mist_examples"

version := "0.0.1"

val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("1.5.2")

scalaVersion := {
  sparkVersion match {
    case versionRegex(major, minor) if major.toInt == 1 && List(3, 4, 5, 6).contains(minor.toInt) => "2.10.6"
    case versionRegex(major, minor) if major.toInt > 1 => "2.11.8"
    case _ => "2.10.6"
  }
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "io.hydrosphere" %% "mist" % "0.3.0"
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
      case versionRegex(major, minor) if major.toInt == 1 && List(4, 5, 6).contains(minor.toInt) => {
        f.getPath.containsSlice("SimpleHiveContext_SparkSession.scala")
      }
      case versionRegex(major, minor) if major.toInt > 1 => {
        f.getPath.containsSlice("SimpleSQLContext.scala") ||
        f.getPath.containsSlice("SimpleHiveContext.scala")
      }
    }
  }
}

excludeFilter in Compile ~= {  _ || exludes }