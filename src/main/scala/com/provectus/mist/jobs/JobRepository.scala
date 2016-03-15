package com.provectus.mist.jobs

import com.provectus.mist.{Specification, Repository}

import scala.collection.mutable.ArrayBuffer

private[mist] trait JobRepository extends Repository[Job]

private[mist] object InMemoryJobRepository extends JobRepository {

  private val _collection = ArrayBuffer.empty[Job]

  override def add(job: Job): Unit = {
    _collection += job
  }

  override def get(specification: Specification[Job]): Option[Job] = {
    val predicate: Job => Boolean = x => specification.specified(x)
    _collection.find(predicate)
  }

  override def filter(specification: Specification[Job]): List[Job] = {
    val predicate: Job => Boolean = x => specification.specified(x)
    _collection.filter(predicate).toList
  }

  override def remove(job: Job): Unit = {
    _collection -= job
  }
}

private[mist] object SQLiteJobRepository extends JobRepository {



  override def add(job: Job): Unit = ???

  override def get(specification: Specification[Job]): Option[Job] = ???

  override def filter(specification: Specification[Job]): List[Job] = ???

  override def remove(job: Job): Unit = ???
}

// TODO: persist repository