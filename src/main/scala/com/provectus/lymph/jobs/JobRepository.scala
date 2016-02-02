package com.provectus.lymph.jobs

import com.provectus.lymph.{Specification, Repository}

import scala.collection.mutable.ArrayBuffer

private[lymph] trait JobRepository extends Repository[LymphJob]

private[lymph] object InMemoryJobRepository extends JobRepository {

  private val _collection = ArrayBuffer.empty[LymphJob]

  override def add(job: LymphJob): Unit = {
    _collection += job
  }

  override def get(specification: Specification[LymphJob]): Option[LymphJob] = {
    val predicate: LymphJob => Boolean = x => specification.specified(x)
    _collection.find(predicate)
  }


  override def filter(specification: Specification[LymphJob]): List[LymphJob] = {
    val predicate: LymphJob => Boolean = x => specification.specified(x)
    _collection.filter(predicate).toList
  }

  override def remove(job: LymphJob): Unit = {
    _collection -= job
  }
}

// TODO: persist repository