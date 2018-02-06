package io.hydrosphere.mist.master.execution.remote

import akka.actor.ActorRef

case class WorkerConnection(id: String, ref: ActorRef)
