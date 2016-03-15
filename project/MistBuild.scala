import sbt._
import Keys._

object MistBuild extends Build {

  lazy val mist = project.in(file("."))

  lazy val examples = project.in(file("./examples")).dependsOn(mist)
}