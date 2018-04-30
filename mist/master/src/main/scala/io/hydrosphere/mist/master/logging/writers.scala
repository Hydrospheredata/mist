package io.hydrosphere.mist.master.logging

import java.nio.file._

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.core.logging.LogEvent
import io.hydrosphere.mist.master.LogStoragePaths

import scala.concurrent.Future
import scala.concurrent.duration._

case class WriteRequest(
  id: String,
  events: Seq[LogEvent]
)

case class LogUpdate(
  jobId: String,
  events: Seq[LogEvent],
  bytesOffset: Long
)
/**
  * Single file writer
  */
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

object WriterActor {

  def props(path: Path): Props = Props(classOf[WriterActor], path)
}

/**
  * Group of file writers
  */
class WritersGroupActor(paths: LogStoragePaths) extends Actor {

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
    val path = paths.pathFor(id)
    val props = WriterActor.props(path).withDispatcher("writers-blocking-dispatcher")
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

object WritersGroupActor {

  def props(paths: LogStoragePaths): Props = {
    Props(classOf[WritersGroupActor], paths)
  }
}

trait LogsWriter {

  def write(from: String, events: Seq[LogEvent]): Future[LogUpdate]

}

object LogsWriter {

  def apply(paths: LogStoragePaths, f: ActorRefFactory): LogsWriter = {
    new LogsWriter {
      val actor = f.actorOf(WritersGroupActor.props(paths), "writers-group")
      implicit val timeout = Timeout(10 second)

      override def write(from: String, events: Seq[LogEvent]): Future[LogUpdate] = {
        val f = actor ? WriteRequest(from, events)
        f.mapTo[LogUpdate]
      }
    }
  }
}

