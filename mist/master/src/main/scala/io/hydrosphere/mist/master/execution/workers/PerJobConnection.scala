package io.hydrosphere.mist.master.execution.workers

import akka.actor.ActorRef
import io.hydrosphere.mist.core.CommonData.RunJobRequest

import scala.concurrent.Future

trait PerJobConnection {
  def id: String
  def whenTerminated: Future[Unit]
  def run(req: RunJobRequest, respond: ActorRef): Unit
  def cancel(id: String, respond: ActorRef): Unit
  def release(): Unit
}

object PerJobConnection {

  abstract class Direct(direct: WorkerConnection) extends PerJobConnection {
    def id: String = direct.id
    def whenTerminated: Future[Unit] = direct.whenTerminated

    def run(req: RunJobRequest, respond: ActorRef): Unit
    def cancel(id: String, respond: ActorRef): Unit
    def release(): Unit
  }

}


