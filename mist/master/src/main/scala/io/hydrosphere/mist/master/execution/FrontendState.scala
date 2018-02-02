package io.hydrosphere.mist.master.execution

import scala.collection.immutable.Queue
import scala.collection.mutable

sealed trait EntryState
case object Waiting extends EntryState
case object Granted extends EntryState
case object Working extends EntryState

class FrontendState[K, V](
  waiting: mutable.LinkedHashMap[K, V],
  granted: Map[K, V],
  working: Map[K, V]
) {

  private def noSuchElement(k: K, name: String): Nothing = throw new NoSuchElementException(s"No such key $k in $name state")

  def enqueue(k: K, v: V): FrontendState[K, V] = {
    waiting.put(k, v)
    new FrontendState(waiting, granted, working)
  }

  def all: Map[K, V] = waiting.toMap ++ working

  def queued: Map[K, V] = waiting.toMap ++ granted
  def active: Map[K, V] = working

  def allSize: Int = waiting.size + working.size
  def inProgressSize: Int = granted.size + working.size

  def toGranted(k: K): FrontendState[K, V] = {
    waiting.remove(k) match {
      case Some(v) => new FrontendState(waiting, granted + (k -> v), working)
      case None => noSuchElement(k, "waiting")
    }
  }

  def toWorking(k: K): FrontendState[K, V] = {
    granted.get(k) match {
      case Some(v) => new FrontendState(waiting, granted -k, working + (k -> v))
      case None => noSuchElement(k, "granted")
    }
  }

  def backToWaiting(k: K): FrontendState[K, V] = {
    granted.get(k) match {
      case Some(v) => new FrontendState(waiting, granted -k, working + (k -> v))
      case None => noSuchElement(k, "granted")
    }
  }

  def done(k: K): FrontendState[K, V] = {
    working.get(k) match {
      case Some(_) => new FrontendState(waiting, granted, working - k)
      case None => noSuchElement(k, "working")
    }
  }

  def getWithState(k: K): Option[(V, EntryState)] = {
    queued.get(k).map(v => v -> Waiting) orElse
    granted.get(k).map(v => v -> Granted) orElse
    working.get(k).map(v => v -> Working)
  }

  def get(k: K): Option[V] = getWithState(k).map(_._1)

  def remove(k: K): FrontendState[K, V] = {
    getWithState(k).map(_._2) match {
      case Some(Waiting) =>
        waiting.remove(k)
        new FrontendState(waiting, granted, working)
      case Some(Granted) => new FrontendState(waiting, granted - k, working)
      case Some(Working) => new FrontendState(waiting, granted, working -k)
      case None => noSuchElement(k, "all")
    }
  }

  def grantNext(n: Int)(f: (K, V) => Unit): FrontendState[K, V] = {
    waiting.take(n).foldLeft(this){
      case (st, (k, v)) =>
        f(k, v)
        st.toGranted(k)
    }
  }

  def hasWaiting(k: K): Boolean = getWithState(k).exists(_._2 == Waiting)
  def hasGranted(k: K): Boolean = getWithState(k).exists(_._2 == Granted)
  def hasWorking(k: K): Boolean = getWithState(k).exists(_._2 == Working)
}

object FrontendState {

  def empty[K,V]: FrontendState[K, V] = new FrontendState(mutable.LinkedHashMap.empty, Map.empty, Map.empty)

  def apply[K, V](values: (K, V)*): FrontendState[K, V] = {
    val map = mutable.LinkedHashMap(values: _*)
    new FrontendState(map, Map.empty, Map.empty)
  }

}

