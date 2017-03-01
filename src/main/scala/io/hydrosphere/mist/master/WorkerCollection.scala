package io.hydrosphere.mist.master

import scala.collection.mutable.ArrayBuffer

case class WorkerLink(uid: String, name: String, address: String, blackSpot: Boolean)

class WorkerCollection {

  class CallbackCollection {

    type NameCallbackPair = (String, WorkerCollection.Callback)

    private val callbacks = ArrayBuffer[NameCallbackPair]()

    def +=(nameCallbackPair: NameCallbackPair): Unit = {
      callbacks += nameCallbackPair
    }

    def -=(nameCallbackPair: NameCallbackPair): Unit = {
      callbacks -= nameCallbackPair
    }

    def apply(name: String): List[WorkerCollection.Callback] = {
      callbacks
        .filter({ (pair) =>
          pair._1 == name
        })
        .map(_._2)
        .toList
    }
  }

  private val workers = scala.collection.mutable.Map[(String, String), (String, Boolean)]()

  private val callbacks = new CallbackCollection()

  def +=(worker: WorkerLink): Unit = {
    workers += ((worker.name, worker.uid) -> (worker.address, worker.blackSpot))
    callbacks(worker.name).foreach { (callback) =>
      callback(worker)
      callbacks -= (worker.name, callback)
    }
  }

  def -=(worker: WorkerLink): Unit = {
    println(worker.name, worker.uid)
    println(workers.toList)
    workers -= ((worker.name, worker.uid))
  }

  def contains(name: String, uid: String): Boolean = {
    workers.contains(name, uid)
  }

  def containsName(name: String): Boolean = {
    val w = workers.find(n => n._1._1 == name && !n._2._2)
    if(w.nonEmpty) {
      workers.contains(w.get._1)
    } else { false }
  }

  def getUIDByName(name: String): String = {
    val w = workers.find(n => n._1._1 == name && !n._2._2)
    if(w.nonEmpty) {
      w.get._1._2
    } else {
      ""
    }
  }

  def getUIDByAddress(address: String): String = {
    val w = workers.find(n => n._2._1 == address)
    if(w.nonEmpty) {
      w.get._1._2
    } else {
      ""
    }
  }

  def getNameByUID(uid: String): String = {
    val w = workers.find(n => n._1._2 == uid)
    if(w.nonEmpty) {
      w.get._1._1
    } else { "" }
  }

  def foreach(f: (WorkerLink) => Unit): Unit = {
    workers.foreach {
      case ((name, uid), (address, blackSpot)) =>
        f(WorkerLink(uid, name, address, blackSpot))
    }
  }
  
  def map[T](f: (WorkerLink) => T): List[T] = {
    workers.map {
      case ((name, uid), (address, blackSpot)) =>
        f(WorkerLink(uid, name, address, blackSpot))
    }.toList
  }

  def setBlackSpotByName(name: String): Unit = {
    val w = workers.find(n => {n._1._1 == name && !n._2._2})
    if(w.nonEmpty) {
      workers -= ((name, w.get._1._2))
      workers += ((name, w.get._1._2) -> (w.get._2._1, true))
    }
  }

  def apply(name: String, uid: String): WorkerLink = {
    WorkerLink(uid, name, workers(name, uid)._1, workers(name, uid)._2)
  }

  def registerCallbackForName(name: String, callback: WorkerCollection.Callback): Unit = {
    val uid = getUIDByName(name)
    if (workers.contains(name, uid)) {
      callback(this(name, uid))
    } else {
      callbacks += (name -> callback)
    }
  }

  def isEmpty: Boolean = {
    workers.isEmpty
  }
}

object WorkerCollection {
  type Callback = (WorkerLink) => Unit

  def apply(): WorkerCollection = {
    new WorkerCollection()
  }
}
