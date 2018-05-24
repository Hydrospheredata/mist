import sbt._
import sbt.Keys._
import scala.sys.process._

object PyProject {

  lazy val pyProjectVersion = settingKey[String]("Py project version")
  lazy val pyName = settingKey[String]("Project name")
  lazy val pyDir = settingKey[File]("Directory with python project")
  lazy val pySources = settingKey[File]("Source directory")
  lazy val pythonVersion = settingKey[String]("Python version")
  lazy val virtualDir = settingKey[File]("Directory for virtual env")

  lazy val pyTest = taskKey[Unit]("Run tests")
  lazy val pySdist = taskKey[File]("Make source distribution")
  lazy val pyBdist = taskKey[File]("Make binary distribution")
  lazy val pyBdistEgg = taskKey[File]("Make binary egg distribution")
  lazy val pypiRepo = taskKey[String]("Pypi repo")
  lazy val pyPublish = taskKey[Unit]("Publish to pypi")

  val settings = {
    Seq(
      pyProjectVersion := {
        val v = version.value
        val out = pyDir.value / pyName.value / "__version__.py"
        val data = s"__version__ = '$v'"
        IO.write(out, data)
        v
      },
      pythonVersion := "2",
      pyDir := baseDirectory.value / "src" / "main" / "python",
      virtualDir := pyDir.value / ("env-" + pyName.value + "-" + pythonVersion.value),
      pySources := pyDir.value / pyName.value,
      pypiRepo := "pypi",
      pyPublish := {
        val repo = pypiRepo.value
        val sdist = pySdist.value
        val path = sdist.getAbsolutePath
        venv(pyDir.value, virtualDir.value, pythonVersion.value)("pip install twine")
        venv(pyDir.value, virtualDir.value, pythonVersion.value)(s"twine upload --repository $repo $path")
      }
    ) ++ inConfig(Test)(Seq(
      pyTest := {
        sLog.value.info(s"Starting python test for ${pyName.value}")
        venv(pyDir.value, virtualDir.value, pythonVersion.value)("python setup.py test")
      }
    )) ++ distTasks(pyName, pyDir, virtualDir, pythonVersion)
  }

  private def distTasks(
    nameKey: SettingKey[String],
    dirKey: SettingKey[File],
    venvKey: SettingKey[File],
    pyVersionKey: SettingKey[String]
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
    nameKey: SettingKey[String],
    dirKey: SettingKey[File],
    venvKey: SettingKey[File],
    pyVersionKey: SettingKey[String],
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
    dirKey: SettingKey[File],
    venvKey: SettingKey[File],
    pyVersionKey: SettingKey[String],
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
