package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.Messages.JobExecution.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.execution.remote.WorkerConnector
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future

class ExecutionMaster(
  status: StatusReporter,
  connectorStarter: (String, ContextConfig) => Future[WorkerConnector],
  frontendFactory: (String, StatusReporter, (String, ContextConfig) => Future[WorkerConnector]) => Props
) extends Actor with ActorLogging {

  override def receive: Receive = process(Map.empty)

  private def process(frontend: Map[String, ActorRef]): Receive = {
    case run: RunJobCommand =>
      val name = run.context.name
      frontend.get(name) match {
        case Some(ref) => ref forward run.request
        case None =>
          val props = frontendFactory(name, status, connectorStarter)
          val ref = context.actorOf(props)
          ref ! ContextFrontend.Event.UpdateContext(run.context)
          ref.forward(run.request)

          val next = frontend + (name -> ref)
          context become process(next)
      }
    case c @ CancelJobCommand(name, req) =>
      frontend.get(name) match {
        case Some(ref) => ref forward c.request
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("UNKNONW TODO"))
      }
  }
}

object ExecutionMaster {

  def props(
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => Future[WorkerConnector],
    frontendFactory: (String, StatusReporter, (String, ContextConfig) => Future[WorkerConnector]) => Props
  ): Props = Props(classOf[ExecutionMaster], status, executorStarter, frontendFactory)

  def props(
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => Future[WorkerConnector]
  ): Props = props(status, executorStarter, ContextFrontend.props)

}
