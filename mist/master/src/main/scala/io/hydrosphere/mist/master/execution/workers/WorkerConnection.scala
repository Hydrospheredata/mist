package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorRef
import io.hydrosphere.mist.core.CommonData.ForceShutdown
import io.hydrosphere.mist.master.execution.WorkerLink

import scala.concurrent.Future

case class WorkerConnection(
  id: String,
  ref: ActorRef,
  data: WorkerLink,
  whenTerminated: Future[Unit]
) {

  def shutdown(): Future[Unit] = {
    ref ! ForceShutdown
    whenTerminated
  }
}

