package io.hydrosphere.mist.master.data.endpoints

import java.nio.file.Path

import com.typesafe.config.{Config, ConfigValueType}
import io.hydrosphere.mist.master.data._
import io.hydrosphere.mist.utils.Logger

import scala.collection.JavaConverters._
import scala.util._

class EndpointsStorage(dir: Path) extends FsStorage[EndpointConfig](dir) {
  self =>

  def withDefaults(defaults: Seq[EndpointConfig]): EndpointsStorage = {
    new EndpointsStorage(dir) {
      override def entries: Seq[EndpointConfig] = defaults ++ self.entries

      override def entry(name: String): Option[EndpointConfig] = defaults.find(_.name == name).orElse(self.entry(name))
    }
  }
}

object EndpointsStorage extends Logger {

  def create(dir: String, default: Config): EndpointsStorage = {
    val defaults = parseConfig(default)
    new EndpointsStorage(checkDirectory(dir)).withDefaults(defaults)
  }

  def parseConfig(config: Config): Seq[EndpointConfig] = {
    def parse(name: String): Try[EndpointConfig] = Try {
      val part = config.getConfig(name)
      EndpointConfig.Repr.fromConfig(name, part)
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
