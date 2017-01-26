package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.jobs.FullJobConfiguration

import scala.collection.JavaConverters._

private[mist] class ConfigurationWrapper(configuration: FullJobConfiguration) {
  def parameters: java.util.HashMap[String, Any] = getAsJava(configuration.parameters)

  def path: String = configuration.path

  def className: String = configuration.className

  private def getAsJava[K, V](map: Map[K, V]): java.util.HashMap[K, Any] = {
    new java.util.HashMap(map.map {
      case (key, value) =>
        value match {
          case v: List[_] => key -> getAsJava(v)
          case v: Map[_, _] => key -> getAsJava(v)
          case _ => key -> value
        }
    }.asJava)
  }
  
  private def getAsJava[V](list: List[V]): java.util.List[Any] = {
    list.map {
      case value: List[_] => value.asJava
      case value: Map[_, _] => getAsJava(value)
      case value => value
    }.asJava
  }
}
