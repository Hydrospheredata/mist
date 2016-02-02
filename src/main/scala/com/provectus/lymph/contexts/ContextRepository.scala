package com.provectus.lymph.contexts

import com.provectus.lymph.{Specification, Repository}

import scala.collection.mutable.ArrayBuffer

private[lymph] trait ContextRepository extends Repository[ContextWrapper]

private[lymph] object InMemoryContextRepository extends ContextRepository {

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

// TODO: persist repository