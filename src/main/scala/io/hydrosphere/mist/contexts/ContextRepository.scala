package io.hydrosphere.mist.contexts

import io.hydrosphere.mist.{Specification, Repository}

import scala.collection.mutable.ArrayBuffer

private[mist] trait ContextRepository extends Repository[ContextWrapper]

private[mist] object InMemoryContextRepository extends ContextRepository {

  val collection = ArrayBuffer.empty[ContextWrapper]

  override def add(sparkContext: ContextWrapper): Unit = {
    collection += sparkContext
  }

  override def remove(sparkContext: ContextWrapper): Unit = {
    collection -= sparkContext
  }

  override def get(specification: Specification[ContextWrapper]): Option[ContextWrapper] = {
    val predicate:ContextWrapper => Boolean = x => specification.specified(x)
    collection.find(predicate)
  }

  override def filter(specification: Specification[ContextWrapper]): List[ContextWrapper] = {
    val predicate:ContextWrapper => Boolean = x => specification.specified(x)
    collection.filter(predicate).toList
  }
}