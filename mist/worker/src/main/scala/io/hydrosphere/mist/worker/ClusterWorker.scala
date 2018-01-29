package io.hydrosphere.mist.worker

import akka.actor._
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.core.CommonData._

import scala.concurrent.duration._

class ClusterWorker(
  name: String,
  contextName: String,
  workerInit: WorkerInitInfo => (NamedContext, Props)
) extends Actor with ActorLogging {

  val cluster = Cluster(context.system)

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    cluster.registerOnMemberRemoved()
    context.setReceiveTimeout(1.minute)
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
      context.setReceiveTimeout(1.minute)

    case ReceiveTimeout =>
      shutdown("Problem with cluster - couldn't join after 1 minute")
  }


  def joined(master: Address): Receive = {
    case info: WorkerInitInfo =>
      context.setReceiveTimeout(Duration.Inf)

      log.info("Received init info {}", info)
      val (nm, worker) = startWorker(info)
      log.info("Worker actor started")
      val ui = SparkUtils.getSparkUiAddress(nm.sparkContext)
      register(master, ui)
      context become initialized(master, worker, info)

    case ReceiveTimeout =>
      shutdown("Problem with cluster - can't receive initial data")
  }

  def initialized(master: Address, worker: ActorRef, info: WorkerInitInfo): Receive = {
    case Terminated(ref) if ref == worker =>
      log.info(s"Worker reference for $name is terminated, leave cluster")
      cluster.leave(cluster.selfAddress)
      context.setReceiveTimeout(15.seconds)

    case MemberRemoved(m, _) if m.address == cluster.selfAddress =>
      context.stop(self)
      cluster.system.terminate()

    case ReceiveTimeout =>
      shutdown("Problem with exiting from cluster - force shutdown")

    case MemberRemoved(m, _) if m.hasRole("master") =>
      shutdown("Master is down. Shutdown now")

    case x if isWorkerMessage(x) =>
      worker forward x

    case GetRunInitInfo =>
      sender() ! info

    case x =>
      log.debug(s"Worker interface received $x")
  }

  private def shutdown(reason: String): Unit = {
    log.info(reason)
    context.stop(self)
    cluster.system.terminate()
  }

  private def shutdown(): Unit = {
    context.stop(self)
    cluster.system.terminate()
  }

  private def isWorkerMessage(msg: Any): Boolean =
    !msg.isInstanceOf[MemberEvent] && !msg.isInstanceOf[GetRunInitInfo]

  private def toManagerSelection(address: Address): ActorSelection =
    cluster.system.actorSelection(RootActorPath(address) / "user" / "workers-manager")

  private def register(address: Address, sparkUi: Option[String]): Unit = {
    toManagerSelection(address) ! WorkerRegistration(name, cluster.selfAddress.toString, sparkUi)
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
