package io.hydrosphere.mist.master.data.endpoints

import com.typesafe.config.{Config, ConfigValueFactory}
import io.hydrosphere.mist.master.data.{ConfigRepr, NamedConfig}
import scala.collection.JavaConverters._

case class EndpointConfig(
  name: String,
  path: String,
  className: String,
  nameSpace: String
) extends NamedConfig

object EndpointConfig {

  implicit val Repr = new ConfigRepr[EndpointConfig] {

    override def toConfig(a: EndpointConfig): Config = {
      import ConfigValueFactory._
      val map = Map(
        "path" -> fromAnyRef(a.path),
        "className" -> fromAnyRef(a.className),
        "namespace" -> fromAnyRef(a.nameSpace)
      )
      fromMap(map.asJava).toConfig
    }

    override def fromConfig(config: Config): EndpointConfig = {
      EndpointConfig(
        config.getString("name"),
        config.getString("path"),
        config.getString("className"),
        config.getString("namespace")
      )
    }
  }

}
