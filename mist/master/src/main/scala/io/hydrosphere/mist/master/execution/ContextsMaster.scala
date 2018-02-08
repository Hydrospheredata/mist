package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.Messages.JobExecution.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}

class ContextsMaster(
  frontendF: ActorF[ContextConfig]
) extends Actor with ActorLogging with ActorFSyntax {

  override def receive: Receive = process(Map.empty)

  private def process(frontend: Map[String, ActorRef]): Receive = {
    case run: RunJobCommand =>
      val name = run.context.name
      val ref = frontend.get(name) match {
        case Some(r) => r
        case None =>
          val ref = frontendF.create(run.context)
          val next = frontend + (name -> ref)
          context become process(next)
          ref
      }
      ref forward run.request

    case c @ CancelJobCommand(name, req) =>
      frontend.get(name) match {
        case Some(ref) => ref forward req
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("Can't cancel job on stopped/unknown context"))
      }
  }

}

object ContextsMaster {

  def props(contextF: ActorF[ContextConfig]): Props = Props(classOf[ContextsMaster], contextF)

}
