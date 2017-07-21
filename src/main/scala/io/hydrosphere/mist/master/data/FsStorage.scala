package io.hydrosphere.mist.master.data

import java.io.File
import java.nio.file.{Files, Path}

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.hydrosphere.mist.master.data.FsStorage._
import io.hydrosphere.mist.master.models.NamedConfig
import io.hydrosphere.mist.utils.{Logger, fs}

import scala.util._

class FsStorage[A <: NamedConfig : ConfigRepr](
  dir: Path,
  renderOptions: ConfigRenderOptions = DefaultRenderOptions
) extends Logger with RwLock {

  self =>

  private val repr = implicitly[ConfigRepr[A]]

  def entries: Seq[A] = {
    def files = dir.toFile.listFiles(fs.mkFilter(_.endsWith(".conf")))

    withReadLock {
      files.map(parseFile).foldLeft(List.empty[A])({
        case (list, Failure(e)) =>
          logger.warn("Invalid configuration", e)
          list
        case (list, Success(context)) => list :+ context
      })
    }
  }

  def entry(name: String): Option[A] = {
    val filePath = dir.resolve(s"$name.conf")

    withReadLock {
      val file = filePath.toFile
      if (file.exists()) parseFile(file).toOption else None
    }
  }

  private def parseFile(f: File): Try[A] = {
    val name = f.getName.replace(".conf", "")
    Try(ConfigFactory.parseFile(f)).map(c => repr.fromConfig(name, c))
  }

  def write(name: String, entry: A): A = {
    val config = repr.toConfig(entry)

    val data = config.root().render(renderOptions)

    val filePath = dir.resolve(s"$name.conf")

    withWriteLock {
      Files.write(filePath, data.getBytes)
      entry
    }
  }

}

object FsStorage {

  val DefaultRenderOptions =
    ConfigRenderOptions.defaults()
      .setComments(false)
      .setOriginComments(false)
      .setJson(false)
      .setFormatted(true)

  def create[A <: NamedConfig : ConfigRepr](path: String): FsStorage[A] = new FsStorage[A](checkDirectory(path))

}
