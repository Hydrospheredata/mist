package io.hydrosphere.mist.master

import java.nio.file.{Path, Paths}

case class MasterAppArguments(
  configPath: String,
  routerConfigPath: String
)

object MasterAppArguments {
  val parser = new scopt.OptionParser[MasterAppArguments]("mist-master") {

    override def errorOnUnknownArgument: Boolean = false

    override def reportWarning(msg: String): Unit = {}

    head("mist-master")

    opt[String]("config").optional().action((x, a) => a.copy(configPath = x))
    opt[String]("router-config").optional().action((x, a) => a.copy(routerConfigPath = x))

  }

  val default: MasterAppArguments = {
    val root = Paths.get(sys.env.getOrElse("MIST_HOME", "."))
    fromPath(root)
  }

  def fromPath(root: Path): MasterAppArguments = MasterAppArguments(
    root.resolve("configs").resolve("default.conf").toString,
    root.resolve("configs").resolve("router.conf").toString
  )


  def parse(args: Seq[String]): Option[MasterAppArguments] = parser.parse(args, default)
}

