import sbt._
import sbt.Keys._

object StageDist {

  import java.nio.file._

  lazy val stageDirectory = taskKey[File]("Target directory")
  lazy val stageActions = taskKey[Seq[StageAction]]("Actions to build stage")
  lazy val basicStage = taskKey[File]("Build stage for basic distributive")
  lazy val runStage = taskKey[File]("Build stage for docker distributive")
  lazy val dockerStage = taskKey[File]("Build stage for docker distributive")
  lazy val packageTar = taskKey[File]("Package stage to tar")

  lazy val settings = Seq(
    stageDirectory := target.value / name.value,
    basicStage := stageBuildTask(basicStage).value,
    dockerStage := stageBuildTask(dockerStage).value,
    runStage := stageBuildTask(runStage).value,
    packageTar := {
      import scala.sys.process._

      val dir = basicStage.value
      val name = dir.getName
      val out = s"$name.tar.gz"

      val ps = Process(Seq("tar", "cvfz", out, name), Some(dir.getParentFile))
      ps.!
      file(out)
    }
  )


  private def stageBuildTask(key: TaskKey[File]): Def.Initialize[Task[File]] = Def.task {
    val log = (streams in key).value.log
    val dir = (stageDirectory in key).value
    if (dir.exists())
      IO.delete(dir)

    if (!dir.exists)
      IO.createDirectory(dir)
    val actions = (stageActions in key).value
    actions.foreach({
      case MkDir(name) => mkDir(name, dir)
      case copy: CpFile => copyToDir(copy, dir)
      case Write(name, data) => IO.write(dir.asPath.resolve(name).toFile, data.getBytes)
    })
    log.info(s"Stage is built at $dir")
    dir
  }

  private def mkDir(name: String, dir: File): Unit = {
    val f = dir.asPath.resolve(name).toFile
    IO.createDirectory(f)
  }

  private def copyToDir(a: CpFile, dir: File): Unit = {
    val path = Paths.get(a.path)
    Option(path.getParent).foreach(p => {
      IO.createDirectory(p.toFile)
    })

    val filePath = dir.asPath.resolve(path)
    val file = filePath.toFile
    if (a.file.isDirectory) {
      if (!file.exists())
        IO.createDirectory(file)

      a.file.listFiles().foreach(f => copyToDir(CpFile(f), file))
    } else {
      Files.copy(a.file.asPath, filePath, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  sealed trait StageAction
  case class CpFile(
    file: File,
    renameTo: Option[String],
    toDir: Option[File]
  ) extends StageAction {

    def as(name: String): CpFile = copy(renameTo = Some(name))
    def to(dir: String): CpFile = copy(toDir = Some(sbt.file(dir)))

    def path: String = {
      val name = renameTo.getOrElse(file.getName)
      toDir.map(_.getPath + "/" + name).getOrElse(name)
    }

  }

  object CpFile {
    def apply(f: File): CpFile = CpFile(f, None, None)
    def apply(s: String): CpFile = CpFile(sbt.file(s), None, None)
  }

  case class Write(name: String, data: String) extends StageAction

  case class MkDir(name: String) extends StageAction
}

