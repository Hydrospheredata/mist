package io.hydrosphere.mist.master.execution

import scala.collection.mutable

sealed trait EntryState
case object Waiting extends EntryState
case object Working extends EntryState

class FrontendState[K, V](
  waiting: mutable.LinkedHashMap[K, V],
  working: Map[K, V]
) {

  private def noSuchElement(k: K, name: String): Nothing = throw new NoSuchElementException(s"No such key $k in $name state")

  def enqueue(k: K, v: V): FrontendState[K, V] = {
    waiting.put(k, v)
    new FrontendState(waiting, working)
  }

  def all: Map[K, V] = waiting.toMap ++ working

  def queued: Map[K, V] = waiting.toMap
  def active: Map[K, V] = working

  def allSize: Int = waiting.size + working.size
  def inProgressSize: Int = working.size

  def nextOption: Option[(K, V)] = waiting.headOption
  def takeNext(n: Int): Seq[V] = waiting.take(n).values.toSeq

  def toWorking(k: K): FrontendState[K, V] = {
    waiting.get(k) match {
      case Some(v) =>
        waiting.remove(k)
        new FrontendState(waiting, working + (k -> v))
      case None => noSuchElement(k, "waiting")
    }
  }

  def backToWaiting(k: K): FrontendState[K, V] = {
    working.get(k) match {
      case Some(v) =>
        waiting.put(k, v)
        new FrontendState(waiting, working - k)
      case None => noSuchElement(k, "granted")
    }
  }

  def getWithState(k: K): Option[(V, EntryState)] = {
    waiting.get(k).map(v => v -> Waiting) orElse
    working.get(k).map(v => v -> Working)
  }

  def get(k: K): Option[V] = getWithState(k).map(_._1)

  def done(k: K): FrontendState[K, V] = {
    getWithState(k).map(_._2) match {
      case Some(Waiting) =>
        waiting.remove(k)
        new FrontendState(waiting, working)
      case Some(Working) => new FrontendState(waiting, working -k)
      case None => noSuchElement(k, "all")
    }
  }

  def hasWaiting(k: K): Boolean = getWithState(k).exists(_._2 == Waiting)
  def hasWorking(k: K): Boolean = getWithState(k).exists(_._2 == Working)
  def has(k: K): Boolean = getWithState(k).isDefined

  def isEmpty: Boolean = waiting.isEmpty && working.isEmpty
}

object FrontendState {

  def empty[K,V]: FrontendState[K, V] = new FrontendState(mutable.LinkedHashMap.empty, Map.empty)

  def apply[K, V](values: (K, V)*): FrontendState[K, V] = {
    val map = mutable.LinkedHashMap(values: _*)
    new FrontendState(map, Map.empty)
  }

}

