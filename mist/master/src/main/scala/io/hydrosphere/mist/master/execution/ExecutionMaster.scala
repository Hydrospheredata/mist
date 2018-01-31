package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.Messages.JobExecution.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.Future

class ExecutionMaster(
  status: ActorRef,
  executorStarter: (String, ContextConfig) => Future[ActorRef],
  frontendsFactory: (String, ActorRef, (String, ContextConfig) => Future[ActorRef]) => Props
) extends Actor with ActorLogging {

  override def receive: Receive = process(Map.empty)

  private def process(frontends: Map[String, ActorRef]): Receive = {
    case run: RunJobCommand =>
      val name = run.context.name
      frontends.get(name) match {
        case Some(ref) => ref forward run.request
        case None =>
          val props = frontendsFactory(name, status, executorStarter)
          val ref = context.actorOf(props)
          ref ! ContextFrontend.Event.UpdateContext(run.context)
          ref.forward(run.request)

          val next = frontends + (name -> ref)
          context become process(next)
      }
    case c @ CancelJobCommand(name, req) =>
      frontends.get(name) match {
        case Some(ref) => ref forward c.request
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("UNKNONW TODO"))
      }
  }
}

object ExecutionMaster {

  def props(
    status: ActorRef,
    executorStarter: (String, ContextConfig) => Future[ActorRef],
    frontendFactory: (String, ActorRef, (String, ContextConfig) => Future[ActorRef]) => Props
  ): Props = Props(classOf[ExecutionMaster], status, executorStarter, frontendFactory)

  def props(
    status: ActorRef,
    executorStarter: (String, ContextConfig) => Future[ActorRef]
  ): Props = props(status, executorStarter, ContextFrontend.props)

}
