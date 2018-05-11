import sbt._
import sbt.Keys._
import scala.sys.process._

object PyProject {

  lazy val pyName = taskKey[String]("Project name")
  lazy val pyDir = taskKey[File]("Directory with python project")
  lazy val pySources = taskKey[File]("Source directory")
  lazy val pythonVersion = taskKey[String]("Python version")
  lazy val virtualDir = taskKey[File]("Directory for virtual env")
  lazy val pyUpdate = taskKey[Unit]("Install deps")
  lazy val pyTest = taskKey[Unit]("Run tests")
  lazy val pySdist = taskKey[File]("Make source distribution")
  lazy val pyBdist = taskKey[File]("Make binary distribution")

  val settings = Seq(
    pythonVersion := "2",
    virtualDir := pyDir.value / ("env-" + pyName.value + "-" + pythonVersion.value),
    pySources := pyDir.value / pyName.value,
    pyUpdate := {
      venv(pyDir.value, virtualDir.value, pythonVersion.value)("pip install .")
    }
  ) ++ inConfig(Test)(Seq(
    pyTest := {
      sLog.value.info(s"Starting python test for ${pyName.value}")
      venv(pyDir.value, virtualDir.value, pythonVersion.value)("python setup.py test")
    }
  ))


  private def venv(dir: File, envDir: File, pV: String)(cmd: String): Unit = {
    val commands = Seq(
      s"cd $dir",
      s"virtualenv ${envDir.toString} -p python$pV",
      s"source ${envDir.toString}/bin/activate",
      cmd
    ).mkString(";")
    Process(
      Seq("/bin/bash", "-c", commands)
    ).run(StdOutLogger).exitValue() match {
      case 0 =>
      case x => throw new RuntimeException(s"command in venv failed, exit code: $x")
    }
  }



}
