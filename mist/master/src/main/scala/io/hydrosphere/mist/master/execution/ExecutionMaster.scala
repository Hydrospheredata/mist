package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.Messages.JobExecution.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.execution.workers.WorkerConnector
import io.hydrosphere.mist.master.execution.status.StatusReporter
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future

class ExecutionMaster(
  status: StatusReporter,
  connectorStarter: (String, ContextConfig) => WorkerConnector,
  frontendFactory: (String, StatusReporter, (String, ContextConfig) => WorkerConnector) => Props
) extends Actor with ActorLogging {

  override def receive: Receive = process(Map.empty)

  private def process(frontend: Map[String, ActorRef]): Receive = {
    case run: RunJobCommand =>
      val name = run.context.name
      val ref = frontend.get(name) match {
        case Some(r) => r
        case None =>
          val ref = createFrontend(run.context)
          val next = frontend + (name -> ref)
          context become process(next)
          ref
      }
      ref forward run.request

    case c @ CancelJobCommand(name, req) =>
      frontend.get(name) match {
        case Some(ref) => ref forward c.request
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("Can't cancel job on stopped/unknown context"))
      }

    case upd: ContextFrontend.Event.UpdateContext =>
      frontend.get(upd.context.name) match {
        case Some(ref) => ref ! upd
        case None =>
      }
  }

  private def createFrontend(ctx: ContextConfig): ActorRef = {
    val props = frontendFactory(ctx.name, status, connectorStarter)
    val ref = context.actorOf(props)
    ref ! ContextFrontend.Event.UpdateContext(ctx)
    ref
  }
}

object ExecutionMaster {

  def props(
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => WorkerConnector,
    frontendFactory: (String, StatusReporter, (String, ContextConfig) => WorkerConnector) => Props
  ): Props = Props(classOf[ExecutionMaster], status, executorStarter, frontendFactory)

  def props(
    status: StatusReporter,
    executorStarter: (String, ContextConfig) => WorkerConnector
  ): Props = props(status, executorStarter, ContextFrontend.props)

}
