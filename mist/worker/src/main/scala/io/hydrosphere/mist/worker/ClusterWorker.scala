package io.hydrosphere.mist.worker

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.duration._

class ClusterWorker(
  id: String,
  regHub: ActorRef,
  workerInit: WorkerInitInfo => (NamedContext, Props)
) extends Actor with ActorLogging {

  override def preStart: Unit = {
    regHub ! ActorRegHub.Register(id)
    log.info("send registration to {}", regHub)
  }

  override def receive: Receive = waitInit

  private def waitInit: Receive = {
    case init:WorkerInitInfo =>
      log.info("received init info, {}", init)
      val (nm, props) = workerInit(init)
      val ref = context.actorOf(props)

      val remoteConnection = sender()

      remoteConnection ! WorkerReady(id)
      context watch remoteConnection
      context watch ref
      log.info("become work")

      context become work(sender(), ref)
  }

  private def work(remote: ActorRef, worker: ActorRef): Receive = {
    case Terminated(ref) if ref == remote =>
      log.warning("Remote connection was terminated - shutdown")
      worker ! PoisonPill
      shutdown()
    case Terminated(ref) if ref == worker =>
      log.info("Underlying worker was terminated - shutdown")
      shutdown()
    case x => worker forward x
  }

  private def shutdown(): Unit = {
    context stop self
    context.system.terminate()
  }
}

object ClusterWorker {

  def props(id: String, regHub: ActorRef, workerInit: WorkerInitInfo => (NamedContext, Props)): Props = {
    Props(classOf[ClusterWorker2], id, regHub, workerInit)
  }
}

