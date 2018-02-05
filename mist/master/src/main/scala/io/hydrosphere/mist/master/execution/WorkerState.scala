package io.hydrosphere.mist.master.execution

import akka.actor.{ActorRef, Address}

object WorkerState {
  /**
    * Worker state: Down, Initialized, Started
    */
  sealed trait WorkerState {
    val timestamp: Long = System.currentTimeMillis()
  }

  final case class Down() extends WorkerState

  final case class Initialized() extends WorkerState

  final case class Started(
    address: Address,
    sparkUi: Option[String]
  ) extends WorkerState

}

