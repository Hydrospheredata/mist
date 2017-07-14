package io.hydrosphere.mist.master.data

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import io.hydrosphere.mist.utils.{Logger, fs}

import scala.util._

trait ConfigRepr[A] {

  def name(a: A): String

  def toConfig(a: A): Config

  def fromConfig(name: String, config: Config): A
}

import FsStorage._

class FsStorage[A : ConfigRepr](
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

  def write(entry: A): A = {
    val name = repr.name(entry)
    val config = repr.toConfig(entry)

    val data = config.root().render(renderOptions)

    val filePath = dir.resolve(s"$name.conf")

    withWriteLock {
      Files.write(filePath, data.getBytes)
      entry
    }
  }

  def withDefaults(defaults: Seq[A]): self.type =
    new FsStorage[A](dir, renderOptions) {
      override def entries: Seq[A] = defaults ++ self.entries

      override def entry(name: String): Option[A] = {
        defaults.find(a => repr.name(a) == name).orElse(self.entry(name))
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

  def create[A: ConfigRepr](path: String): FsStorage[A] = new FsStorage[A](checkDirectory(path))

}
