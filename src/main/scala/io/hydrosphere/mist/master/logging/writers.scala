package io.hydrosphere.mist.master.logging

import java.nio.file._

import akka.actor._

import scala.concurrent.duration._

class WriterActor(path: Path) extends Actor {

  override def preStart(): Unit = {
    context.setReceiveTimeout(1 minute)
  }

  override def receive: Receive = {
    case WriteRequest(id, events) =>
      val data = events.map(_.mkString).mkString("", "\n", "\n")

      val f = path.toFile
      if (!f.exists())
        Files.createFile(path)

      Files.write(path, data.getBytes, StandardOpenOption.APPEND)

      sender() ! LogUpdate(id, events, f.length())

    case ReceiveTimeout =>
      context.stop(self)
  }
}

class WritersGroup(mappings: LogStorageMappings) extends Actor {

  var idToRef = Map.empty[String, ActorRef]
  var refToId = Map.empty[ActorRef, String]

  override def receive: Receive = {
    case req: WriteRequest =>
      idToRef.get(req.id) match {
        case Some(ref) => ref.forward(req)
        case None => create(req.id).forward(req)
      }

    case Terminated(ref) => remove(ref)
  }

  def create(id: String): ActorRef = {
    val path = mappings.pathFor(id)
    val props = Props(classOf[WriterActor], path).withDispatcher("writers-blocking-dispatcher")
    val actor = context.actorOf(props, s"writer-$id")
    context.watch(actor)

    idToRef += id -> actor
    refToId += actor -> id
    actor
  }

  def remove(ref: ActorRef): Unit = {
    refToId.get(ref).foreach(id => {
      refToId -= ref
      idToRef -= id
    })
  }
}

object WritersGroup {

  def props(mappings: LogStorageMappings): Props = {
    Props(classOf[WritersGroup], mappings)
  }
}

