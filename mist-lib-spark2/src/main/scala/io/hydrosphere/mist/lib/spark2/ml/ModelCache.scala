package io.hydrosphere.mist.lib.spark2.ml

import org.apache.spark.ml.Transformer

import scala.collection.mutable

object ModelCache {
  
  private val cache: mutable.HashMap[String, Any] = new mutable.HashMap[String, Any] 
  
  def add[T](model: T): Unit = {
    add(model.asInstanceOf[Transformer].uid, model)
  }

  def add[T](key: String, model: T): Unit = {
    if (!cache.contains(key)) {
      cache += key -> model
    }
  }
  
  def get[T](key: String): Option[T] = {
    cache.get(key).map(_.asInstanceOf[T])
  }
  
}
