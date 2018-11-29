package io.hydrosphere.mist.utils.akka

import akka.actor._

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{Duration, FiniteDuration}

trait ActorRegHub {
  def regPath: ActorPath
  def waitRef(id: String, timeout: Duration): Future[ActorRef]
}

object ActorRegHub {

  case class Register(id: String)
  case class Expect(id: String, timeout: Duration, promise: Promise[ActorRef])
  case class Timeout(id: String)

  class WaiterHubActor extends Actor with ActorLogging {

    override def receive: Receive = process(Map.empty)

    private def process(exps: Map[String, Expect]): Receive = {
      case Register(id) => exps.get(id) match {
        case Some(expect) =>
          expect.promise.success(sender())
          become(exps - id)
        case None => log.warning(s"Received unexpected registration with id $id")
      }

      case ex: Expect =>
        ex.timeout match {
          case f: FiniteDuration =>
            context.system.scheduler.scheduleOnce(f, self, Timeout(ex.id))(context.dispatcher)
          case _ =>
        }
        become(exps + (ex.id -> ex))

      case Timeout(id) => exps.get(id) match {
        case Some(ex) =>
          val err = new RuntimeException(s"Registration for $id wasn't happened for ${ex.timeout}")
          ex.promise.failure(err)
          become(exps - id)
        case None =>
      }
    }

    private def become(exps: Map[String, Expect]): Unit = {
      context.become(process(exps))
    }

  }


  def apply(name: String, sys: ActorSystem): ActorRegHub = {
    val props = Props(classOf[WaiterHubActor])
    new ActorRegHub {
      val actor = sys.actorOf(props, name)

      override def waitRef(id: String, timeout: Duration): Future[ActorRef] = {
        val promise = Promise[ActorRef]
        actor ! Expect(id, timeout, promise)
        promise.future
      }

      override def regPath: ActorPath = actor.path
    }
  }

}
