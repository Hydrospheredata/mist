import sbt._
import Keys._

object MistBuild extends Build {

  lazy val mist = project

  lazy val examples = project in file("examples") dependsOn(mist)

}