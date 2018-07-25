package io.hydrosphere.mist.master.data

import java.io.File
import java.nio.file.{Files, Path}
import java.util.concurrent.Executors

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import io.hydrosphere.mist.master.ContextsSettings
import io.hydrosphere.mist.master.data.FsStorage._
import io.hydrosphere.mist.master.models.{ContextConfig, NamedConfig}
import io.hydrosphere.mist.utils.{Logger, fs}

import scala.concurrent.{ExecutionContext, Future}
import scala.util._

class FsStorage[A <: NamedConfig](
  dir: Path,
  repr: ConfigRepr[A],
  renderOptions: ConfigRenderOptions = DefaultRenderOptions
) extends Logger with RwLock { self =>

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

  def delete(name: String): Option[A] = {
    entry(name).map(e => {
      withWriteLock { Files.delete(filePath(name)) }
      e
    })
  }

  private def filePath(name: String): Path = dir.resolve(s"$name.conf")

  private def parseFile(f: File): Try[A] = {
    val name = f.getName.replace(".conf", "")
    Try(ConfigFactory.parseFile(f)).map(c => repr.fromConfig(name, c))
  }

  def write(name: String, entry: A): A = {
    val config = repr.toConfig(entry)

    val data = config.root().render(renderOptions)

    withWriteLock {
      Files.write(filePath(name), data.getBytes)
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

  def create[A <: NamedConfig](path: String, repr: ConfigRepr[A]): FsStorage[A] =
    new FsStorage[A](checkDirectory(path), repr)

}

