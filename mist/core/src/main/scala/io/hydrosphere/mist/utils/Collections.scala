package io.hydrosphere.mist.utils

import java.util

import scala.collection.JavaConverters._

object Collections {

  def asJavaRecursively[K, V](map: Map[K, V]): java.util.HashMap[K, Any] = {
    new java.util.HashMap(map.map {
      case (key, value) =>
        value match {
          case v: Seq[_] => key -> asJavaRecursively(v)
          case v: Map[_, _] => key -> asJavaRecursively(v)
          case _ => key -> value
        }
    }.asJava)
  }

  def asJavaRecursively[V](list: Seq[V]): java.util.List[Any] = {
    list.map {
      case value: Seq[_] => value.asJava
      case value: Map[_, _] => asJavaRecursively(value)
      case value => value
    }.asJava
  }
  
  def asScalaRecursively[K, V](map: java.util.Map[K, V]): Map[K, Any] = {
    map.asScala.map {
      case (key, value) =>
        value match {
          case v: java.util.HashMap[_, _] => key -> asScalaRecursively(v)
          case v: java.util.Map[_, _] => key -> asScalaRecursively(v)
          case v: java.util.List[_] => key -> asScalaRecursively(v)
          case _ => key -> value
        }
    }.toMap[K, Any]
  }

  def asScalaRecursively[K, V](map: java.util.HashMap[K, V]): Map[K, Any] = {
    map.asScala.map {
      case (key, value) =>
        value match {
          case v: java.util.HashMap[_, _] => key -> asScalaRecursively(v)
          case v: java.util.Map[_, _] => key -> asScalaRecursively(v)
          case v: java.util.List[_] => key -> asScalaRecursively(v)
          case _ => key -> value
        }
    }.toMap[K, Any]
  }

  def asScalaRecursively[V](list: java.util.List[V]): List[Any] = {
    list.asScala.map {
      case value: java.util.List[_] => asScalaRecursively(value)
      case value: java.util.HashMap[_, _] => asScalaRecursively(value)
      case value => value
    }.toList
  }
  
}
