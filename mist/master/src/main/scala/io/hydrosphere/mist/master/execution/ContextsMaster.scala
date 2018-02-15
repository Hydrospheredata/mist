package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.Messages.JobExecution.{CancelJobCommand, RunJobCommand}
import io.hydrosphere.mist.master.execution.ContextFrontend.Event.UpdateContext
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}

class ContextsMaster(
  frontendF: ActorF[ContextConfig]
) extends Actor with ActorLogging with ActorFSyntax {

  type State = Map[String, ActorRef]

  override def receive: Receive = process(Map.empty)

  private def process(state: State): Receive = {
    case run: RunJobCommand =>
      val (next, ref) = getOrCreate(state, run.context)
      ref forward run.request
      context become process(next)

    case c @ CancelJobCommand(name, req) =>
      state.get(name) match {
        case Some(ref) => ref forward req
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("Can't cancel job on stopped/unknown context"))
      }

    case upd @ ContextFrontend.Event.UpdateContext(ctx) =>
      state.get(ctx.name) match {
        case Some(ref) => ref forward upd
        case None => getOrCreate(state, ctx)
      }

  }

  private def getOrCreate(state: State, ctx: ContextConfig): (State, ActorRef) = {
    state.get(ctx.name) match {
      case Some(r) => (state, r)
      case None =>
        val ref = frontendF.create(ctx)
        val next = state + (ctx.name -> ref)
        context watch ref
        (next, ref)
    }
  }

}

object ContextsMaster {

  def props(contextF: ActorF[ContextConfig]): Props = Props(classOf[ContextsMaster], contextF)

}
