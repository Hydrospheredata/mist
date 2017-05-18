import sbt._
import sbt.Keys._
import StageDistKeys._
import StageDist._

object StageDist {

  sealed trait StageAction

  case class CopyFile(
    file: File,
    renameTo: Option[String],
    toDir: Option[File]
  ) extends StageAction {

    def as(name: String): CopyFile = copy(renameTo = Some(name))
    def to(dir: String): CopyFile = copy(toDir = Some(sbt.file(dir)))

    def path: String = {
      val name = renameTo.getOrElse(file.getName)
      toDir.map(_.getPath + "/" + name).getOrElse(name)
    }

  }

  object CopyFile {
    def apply(f: File): CopyFile = CopyFile(f, None, None)
    def apply(s: String): CopyFile = CopyFile(sbt.file(s), None, None)
  }

  case class MkDir(name: String) extends StageAction

}

object StageDistKeys {
  lazy val stageDirectory = taskKey[File]("Target directory")
  lazy val stageActions = taskKey[Seq[StageAction]]("Actiions to build stage")
  lazy val stageBuild = taskKey[File]("Build stage distributive")
  lazy val stageClean = taskKey[Unit]("Clean stage directory")

  lazy val packageTar = taskKey[File]("Package stage to zip")
}

object StageDistSettings {

  import java.nio.file._

  lazy val settings = Seq(
    stageDirectory := target.value / name.value,
    stageBuild := {
      val log = streams.value.log
      val dir = stageDirectory.value
      if (!dir.exists())
        IO.createDirectory(dir)
      stageActions.value.foreach({
        case MkDir(name) => mkDir(name, dir)
        case copy: CopyFile => copyToDir(copy, dir)
      })
      log.info(s"Stage is builded at $dir")
      dir
    },
    stageClean := {
      IO.delete(stageDirectory.value)
    },
    packageTar := {
      import scala.sys.process._

      val dir = stageBuild.value
      val name = dir.getName
      val out = s"$name.tar.gz"

      val ps = Process(Seq("tar", "cvfz", out, name), Some(dir.getParentFile))
      ps.!
      file(out)
    }
  )

  private def mkDir(name: String, dir: File): Unit = {
    val f = dir.asPath.resolve(name).toFile
    IO.createDirectory(f)
  }

  private def copyToDir(a: CopyFile, dir: File): Unit = {
    val path = Paths.get(a.path)
    Option(path.getParent).foreach(p => {
      IO.createDirectory(p.toFile)
    })

    val filePath = dir.asPath.resolve(path)
    val file = filePath.toFile
    if (a.file.isDirectory) {
      if (!file.exists())
        IO.createDirectory(file)

      a.file.listFiles().foreach(f => copyToDir(CopyFile(f), file))
    } else {
      Files.copy(a.file.asPath, filePath, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  implicit class AsPath(f: File) {
    def asPath: Path = Paths.get(f.getAbsolutePath)
  }

}


