package io.hydrosphere.mist.worker

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.WorkerMessages.{GetRunInitInfo, WorkerInitInfo, WorkerInitInfoReq, WorkerRegistration}

import scala.reflect.ClassTag

class ClusterWorker(
  name: String,
  contextName: String,
  workerInit: WorkerInitInfo => (NamedContext, Props)
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def startWorker(initInfo: WorkerInitInfo): (NamedContext, ActorRef) = {
    val (nm, props) = workerInit(initInfo)
    val ref = context.actorOf(props)
    context.watch(ref)
    (nm, ref)
  }

  def receive = initial

  def initial: Receive = {
    case MemberUp(m) if m.hasRole("master") =>
      log.info(s"Joined to cluster. Master ${m.address}")
      requestInfo(m.address)
      context become joined(m.address)
  }


  def joined(master: Address): Receive = {
    case info: WorkerInitInfo =>
      log.info("Received init info {}", info)
      val (nm, worker) = startWorker(info)
      log.info("Worker actor started")
      val ui = SparkUtils.getSparkUiAddress(nm.sparkContext)
      register(master, ui)
      context become initialized(master, worker, info)
  }

  def initialized(master: Address, worker: ActorRef, info: WorkerInitInfo): Receive = {
    case Terminated(ref) if ref == worker =>
      log.info(s"Worker reference for $name is terminated, leave cluster")
      cluster.leave(cluster.selfAddress)

    case MemberRemoved(m, _) if m.address == cluster.selfAddress =>
      context.stop(self)
      cluster.unsubscribe(self)
      cluster.system.shutdown()

    case MemberRemoved(m, _) if m.hasRole("master") =>
      log.info("Master is down. Shutdown now")
      context.stop(self)
      cluster.system.shutdown()

    case x if isWorkerMessage(x) =>
      worker forward x

    case GetRunInitInfo =>
      sender() ! info

    case x =>
      log.debug(s"Worker interface received $x")

  }
  private def isWorkerMessage(msg: Any): Boolean =
    !msg.isInstanceOf[MemberEvent] && !msg.isInstanceOf[GetRunInitInfo]

  private def toManagerSelection(address: Address): ActorSelection =
    cluster.system.actorSelection(RootActorPath(address) / "user" / "workers-manager")

  private def register(address: Address, sparkUi: Option[String]): Unit = {
    toManagerSelection(address) ! WorkerRegistration(name, cluster.selfAddress, sparkUi)
  }

  private def requestInfo(address: Address): Unit = {
    toManagerSelection(address) ! WorkerInitInfoReq(contextName)
  }


}

object ClusterWorker {

  def props(name: String, contextName: String, workerInit: WorkerInitInfo => (NamedContext, Props)): Props = {
    Props(classOf[ClusterWorker], name, contextName, workerInit)
  }

}
