package com.provectus.lymph.actors

import akka.actor.Actor
import com.provectus.lymph.actors.tools.Messages.{CreateContext, RemoveAllContexts}
import com.provectus.lymph.contexts._

private[lymph] class ContextManager extends Actor {
  override def receive: Receive = {
    case message: CreateContext =>
      val existedContext: ContextWrapper = InMemoryContextRepository.get(new NamedContextSpecification(message.name)) match {
        case Some(contextWrapper) => contextWrapper
        case None =>
          println(s"creating context ${message.name}")
          val contextWrapper = ContextBuilder.namedSparkContext(message.name)

          InMemoryContextRepository.add(contextWrapper)
          contextWrapper
      }

      if (sender.path.toString != "akka://lymph/deadLetters") {
        sender ! existedContext
      }

    case RemoveAllContexts =>
      InMemoryContextRepository.filter(new DummyContextSpecification()).foreach(_.stop())
  }
}
