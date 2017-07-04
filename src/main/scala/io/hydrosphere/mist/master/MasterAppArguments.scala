package io.hydrosphere.mist.master

import java.nio.file.Paths

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

  val default = MasterAppArguments(
    Paths.get("configs", "default.conf").toString,
    Paths.get("configs", "router.conf").toString
  )

  def parse(args: Seq[String]): Option[MasterAppArguments] = parser.parse(args, default)
}

