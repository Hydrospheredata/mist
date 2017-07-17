package io.hydrosphere.mist.master.data

import com.typesafe.config.{Config, ConfigValueFactory}

trait NamedConfig {
  val name: String
}

trait ConfigRepr[A <: NamedConfig] {

  def toConfig(a: A): Config

  def fromConfig(config: Config): A

  def fromConfig(name: String, config: Config): A =
    fromConfig(config.withValue("name", ConfigValueFactory.fromAnyRef(name)))
}

