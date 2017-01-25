package io.hydrosphere.mist.jobs.runners.python.wrappers

import scala.collection.JavaConverters._

private[mist] class DataWrapper {
  private var data: Map[String, Any] = _

  def set(in: Map[String, Any]): Unit = {
    data = in
  }
  
  def get: java.util.Map[String, Any] = getAsJava(data)
  
  private def getAsJava[K, V](map: Map[K, V]): java.util.Map[K, Any] = {
    map.map {
      case (key, value) =>
        value match {
          case v: List[_] => key -> v.asInstanceOf[java.util.List[_]]
          case v: Map[_, _] => key -> getAsJava(v)
          case _ => key -> value
        }
    }.asJava
  }
}
