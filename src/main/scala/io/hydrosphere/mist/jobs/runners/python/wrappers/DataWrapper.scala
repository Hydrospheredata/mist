package io.hydrosphere.mist.jobs.runners.python.wrappers

import scala.collection.JavaConverters._

private[mist] class DataWrapper {
  private var data: java.util.HashMap[String, Any] = _

  def set(in: java.util.HashMap[String, Any]): Unit = {
    println(in.getClass.getCanonicalName)
    data = in
  }
  def get: Map[String, Any] = getAsScala(data)
  
  private def getAsScala[K, V](map: java.util.HashMap[K, V]): Map[K, Any] = {
    map.asScala.map {
      case (key, value) =>
        value match {
          case v: java.util.HashMap[_, _] => key -> getAsScala(v)
          case v: java.util.List[_] => key -> getAsScala(v)
          case _ => key -> value
        }
    }.toMap[K, Any]
  }
  
  private def getAsScala[V](list: java.util.List[V]): List[Any] = {
    list.asScala.map {
      case value: java.util.List[_] => getAsScala(value)
      case value: java.util.HashMap[_, _] => getAsScala(value)
      case value => value
    }.toList
  }
}
