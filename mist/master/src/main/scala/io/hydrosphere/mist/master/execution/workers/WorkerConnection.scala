package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorRef
import io.hydrosphere.mist.master.WorkerLink

import scala.concurrent.Future

case class WorkerConnection(
  id: String,
  ref: ActorRef,
  data: WorkerLink,
  whenTerminated: Future[Unit]
)

