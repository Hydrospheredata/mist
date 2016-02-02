import sbt._
import Keys._

object LymphBuild extends Build {

  lazy val lymph = project.in(file("."))

  lazy val examples = project.in(file("./examples")).dependsOn(lymph)
}