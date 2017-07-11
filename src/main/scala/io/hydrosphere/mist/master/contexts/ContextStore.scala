package io.hydrosphere.mist.master.contexts

import java.io.File
import java.nio.file.{Files, Path, Paths}

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import io.hydrosphere.mist.utils.Logger

import scala.util.{Failure, Success, Try}

trait ContextStore {

  def contexts: Seq[ContextConfig]

  def configFor(name: String): ContextConfig

  def precreated: Seq[ContextConfig] = contexts.filter(_.precreated)

}

class DirectoryContextStore(
  dir: Path,
  default: ContextConfig,
  fallbackTo: Config
) extends ContextStore with Logger {

  import io.hydrosphere.mist.utils.fs

  override def contexts: Seq[ContextConfig] = {
    val files = dir.toFile.listFiles(fs.mkFilter(_.endsWith(".conf")))
    files.map(parseFile).foldLeft(List.empty[ContextConfig])({
      case (list, Failure(e)) =>
        logger.warn("Invalid context configuration", e)
        list
      case (list, Success(context)) => list :+ context
    })
  }

  private def parseFile(f: File): Try[ContextConfig] = {
    val name = f.getName.replace(".conf", "")
    for {
      raw <- Try(ConfigFactory.parseFile(f))
      context = ContextConfig.fromConfig(name, raw.withFallback(fallbackTo))
    } yield context
  }

  override def configFor(name: String): ContextConfig =
    contexts.find(_.name == name).getOrElse(default)

  def save(context: ContextConfig): ContextConfig = {
    val options = ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)
      .setFormatted(true)

    val data = context.toRaw.root().render(options)

    val filePath = dir.resolve(s"${context.name}.conf")
    Files.write(filePath, data.getBytes)

    context
  }
}

object DirectoryContextStore {

  def create(
    path: String,
    default: ContextConfig,
    fallbackTo: Config
  ): DirectoryContextStore = {
    val p = Paths.get(path)
    val dir = p.toFile
    if (!dir.exists()) dir.mkdir()
    else if (dir.isFile) throw new IllegalArgumentException(s"Path $path already exists as file")

    new DirectoryContextStore(p, default, fallbackTo)
  }
}
