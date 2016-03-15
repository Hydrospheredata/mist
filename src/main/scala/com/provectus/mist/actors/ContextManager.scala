package com.provectus.mist.actors

import akka.actor.Actor
import com.provectus.mist.actors.tools.Messages.{RemoveContext, CreateContext, StopAllContexts}
import com.provectus.mist.contexts._

/** Manages context repository */
private[mist] class ContextManager extends Actor {
  override def receive: Receive = {
    // Returns context if it exists in requested namespace or created a new one if not
    case message: CreateContext =>
      val existedContext: ContextWrapper = InMemoryContextRepository.get(new NamedContextSpecification(message.name)) match {
        // return existed context
        case Some(contextWrapper) => contextWrapper
        // create new context
        case None =>
          println(s"creating context ${message.name}")
          val contextWrapper = ContextBuilder.namedSparkContext(message.name)

          InMemoryContextRepository.add(contextWrapper)
          contextWrapper
      }

      // if sender is asking, send it result
      if (sender.path.toString != "akka://mist/deadLetters") {
        sender ! existedContext
      }

    // surprise: stops all contexts
    case StopAllContexts =>
      InMemoryContextRepository.filter(new DummyContextSpecification()).foreach(_.stop())

    // removes context
    case message: RemoveContext =>
      message.context.context.stop()
      InMemoryContextRepository.remove(message.context)
  }
}
