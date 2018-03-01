package io.hydrosphere.mist.master.execution

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import io.hydrosphere.mist.master.models.ContextConfig
import io.hydrosphere.mist.utils.akka.{ActorF, ActorFSyntax}

class ContextsMaster(
  frontendF: ActorF[ContextConfig]
) extends Actor with ActorLogging with ActorFSyntax {

  type State = Map[String, ActorRef]

  override def receive: Receive = process(Map.empty)

  private def process(state: State): Receive = {
    case run: ContextEvent.RunJobCommand =>
      val (next, ref) = getOrCreate(state, run.context)
      ref forward run.request
      context become process(next)

    case c @ ContextEvent.CancelJobCommand(name, req) =>
      state.get(name) match {
        case Some(ref) => ref forward req
        case None => sender() ! akka.actor.Status.Failure(new IllegalStateException("Can't cancel job on stopped/unknown context"))
      }

    case upd @ ContextEvent.UpdateContext(ctx) =>
      state.get(ctx.name) match {
        case Some(ref) => ref forward upd
        case None =>
          val (next, ref) = getOrCreate(state, ctx)
          context become process(next)
      }

    case ContextsMaster.ContextTerminated(name) =>
      val next = state - name
      context become process(next)
  }

  private def getOrCreate(state: State, ctx: ContextConfig): (State, ActorRef) = {
    state.get(ctx.name) match {
      case Some(r) => (state, r)
      case None =>
        val ref = frontendF.create(ctx)
        val next = state + (ctx.name -> ref)
        context.watchWith(ref, ContextsMaster.ContextTerminated(ctx.name))
        (next, ref)
    }
  }

}


object ContextsMaster {

  case class ContextTerminated(name: String)

  def props(contextF: ActorF[ContextConfig]): Props = Props(classOf[ContextsMaster], contextF)

}
