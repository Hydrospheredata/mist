import sbt._
import sbt.Keys._
import scala.sys.process._

object PyProject {

  lazy val pyName = taskKey[String]("Project name")
  lazy val pyDir = taskKey[File]("Directory with python project")
  lazy val pySources = taskKey[File]("Source directory")
  lazy val pythonVersion = taskKey[String]("Python version")
  lazy val virtualDir = taskKey[File]("Directory for virtual env")
  lazy val pyTest = taskKey[Unit]("Run tests")
  lazy val pySdist = taskKey[File]("Make source distribution")
  lazy val pyBdist = taskKey[File]("Make binary distribution")
  lazy val pyBdistEgg = taskKey[File]("Make binary egg distribution")

  val settings = Seq(
    pythonVersion := "2",
    pyDir := baseDirectory.value / "src" / "main" / "python",
    virtualDir := pyDir.value / ("env-" + pyName.value + "-" + pythonVersion.value),
    pySources := pyDir.value / pyName.value
  ) ++ inConfig(Test)(Seq(
    pyTest := {
      sLog.value.info(s"Starting python test for ${pyName.value}")
      venv(pyDir.value, virtualDir.value, pythonVersion.value)("python setup.py test")
    }
  )) ++ distTasks(pyName, pyDir, virtualDir, pythonVersion)

  private def distTasks(
    nameKey: TaskKey[String],
    dirKey: TaskKey[File],
    venvKey: TaskKey[File],
    pyVersionKey: TaskKey[String]
  ): Seq[Def.Setting[_]] = {
    Seq(
      pyBdist -> "bdist",
      pyBdistEgg -> "bdist_egg",
      pySdist -> "sdist"
    ).map({case (key, cmd) =>
      key := mkDistTask(nameKey, dirKey, venvKey, pyVersionKey, cmd).value
    })
  }

  private def mkDistTask(
    nameKey: TaskKey[String],
    dirKey: TaskKey[File],
    venvKey: TaskKey[File],
    pyVersionKey: TaskKey[String],
    distType: String
  ): Def.Initialize[Task[File]] = Def.task {
    Def.sequential(
      Def.task {
        val dir = pyDir.value / "dist"
        if (dir.exists()) IO.delete(dir)
      },
      inVenv(pyDir, virtualDir, pythonVersion, s"python setup.py $distType")
    ).value

    val name = pyName.value
    IO.listFiles(pyDir.value / "dist").map(x => x).find(f => f.name.startsWith(name)) match {
      case Some(f) => f
      case None => throw new RuntimeException(s"Could'n find dist file for $name")
    }
  }

  private def inVenv(
    dirKey: TaskKey[File],
    venvKey: TaskKey[File],
    pyVersionKey: TaskKey[String],
    cmd: String
  ): Def.Initialize[Task[Unit]] = Def.task(venv(dirKey.value, venvKey.value, pyVersionKey.value)(cmd))

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
