package io.hydrosphere.mist.master.data

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import com.typesafe.config.{ConfigValueFactory, ConfigFactory, ConfigValueType, Config}
import io.hydrosphere.mist.master.models.EndpointConfig
import io.hydrosphere.mist.utils.Logger

import scala.collection.JavaConverters._
import scala.concurrent.{Future, ExecutionContext}
import scala.util._

class EndpointsStorage(
  fsStorage: FsStorage[EndpointConfig],
  val defaults: Seq[EndpointConfig]
)(implicit ex: ExecutionContext) {

  private val defaultMap = defaults.map(e => e.name -> e).toMap

  def all: Future[Seq[EndpointConfig]] =
    Future { fsStorage.entries } map (seq => {
      val merged = defaultMap ++ seq.map(a => a.name -> a).toMap
      merged.values.toSeq
    })

  def get(name: String): Future[Option[EndpointConfig]] = {
    Future { fsStorage.entry(name) } flatMap {
      case s @ Some(_) => Future.successful(s)
      case None => Future.successful(defaultMap.get(name))
    }
  }

  def update(ec: EndpointConfig): Future[EndpointConfig] =
    Future { fsStorage.write(ec.name, ec) }

}

object EndpointsStorage extends Logger {

  def create(
    storagePath: String,
    defaultConfigPath: String): EndpointsStorage = {

    val defaults = fromDefaultsConfig(defaultConfigPath)
    val fsStorage = new FsStorage(checkDirectory(storagePath), ConfigRepr.EndpointsRepr)
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))
    new EndpointsStorage(fsStorage, defaults)(ec)
  }

  def fromDefaultsConfig(path: String): Seq[EndpointConfig] = {
    val file = new File(path)
    if (!file.exists()) {
      Seq.empty
    } else {
      logger.warn("Starting using router conf (that feature will be removed - please use http api for uploading functions)")
      val directory = Paths.get(path).getParent
      val config = ConfigFactory.parseFile(file)
        .withValue("location", ConfigValueFactory.fromAnyRef(directory.toString))
        .resolve()
      parseConfig(config)
    }
  }

  def parseConfig(config: Config): Seq[EndpointConfig] = {
    def parse(name: String): Try[EndpointConfig] = Try {
      val part = config.getConfig(name)
      ConfigRepr.EndpointsRepr.fromConfig(name, part)
    }

    config.root().keySet().asScala
      .filter(k => config.getValue(k).valueType() == ConfigValueType.OBJECT)
      .map(name => parse(name))
      .foldLeft(List.empty[EndpointConfig])({
        case (lst, Failure(e)) =>
          logger.warn("Invalid configuration for endpoint", e)
          lst
        case (lst, Success(c)) => lst :+ c
      })
  }
}
