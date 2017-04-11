package io.hydrosphere.mist.worker

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.WorkerMessages.WorkerRegistration

class ClusterWorker(
  name: String,
  underlying: Props
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def startWorker(): ActorRef = {
    val ref = context.actorOf(underlying)
    context.watch(ref)
    ref
  }

  def receive = initial

  def initial: Receive = {
    case MemberUp(m) if m.hasRole("master") =>
      log.info(s"Joined to cluster. Master ${m.address}")
      val worker = startWorker()
      register(m.address)
      context become joined(m.address, worker)
  }

  def joined(master: Address, worker: ActorRef): Receive = {
    case Terminated(ref) if ref == worker =>
      log.info(s"Worker reference for $name is terminated, leave cluster")
      cluster.leave(cluster.selfAddress)

    case MemberRemoved(m, _) if m.address == cluster.selfAddress =>
      context.stop(self)
      cluster.system.shutdown()

    case x if !x.isInstanceOf[MemberEvent] =>
      worker forward x

    case x =>
      log.debug(s"Worker interface received $x")
  }

  private def toManagerSelection(address: Address): ActorSelection =
    cluster.system.actorSelection(RootActorPath(address) / "user" / "workers-manager")

  private def register(address: Address): Unit = {
    toManagerSelection(address) ! WorkerRegistration(name, cluster.selfAddress)
  }


}

object ClusterWorker {

  def props(name: String, workerProps: Props): Props = {
    Props(classOf[ClusterWorker], name, workerProps)
  }

}
