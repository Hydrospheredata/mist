package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorRefFactory, Props, Terminated}
import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.models.{ContextConfig, RunMode}
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.concurrent.Future

//class RemoteExecutorStarter(
//  regHub: ActorRegHub
//) {
//
//}
//
//trait RemoteStarter {
//  def start(config: ContextConfig, spawnSettings: SpawnSettings): Future[ActorRef]
//}
//
//sealed trait WorkerState
//case object Starting extends WorkerState
//case class Working(ref: ActorRef) extends WorkerState
//
//object RemoteExecutor {
//
//  // simple proxy to shared worker
//  class SharedProxy(target: ActorRef) extends Actor with ActorLogging {
//
//    override def preStart(): Unit = context watch target
//
//    override def receive: Receive = {
//      case msg => target forward msg
//      case Terminated(_) => context.stop(self)
//    }
//  }
//
//  object SharedProxy {
//    def apply(target: ActorRef): Props = Props(classOf[SharedProxy], target)
//  }
//
//
//  class ExclusivePool(
//    context: ContextConfig,
//    spawnSettings: SpawnSettings,
//    regHub: ActorRegHub
//  ) extends Actor with ActorLogging {
//
//    override def receive: Receive = {
//
//    }
//
//    private def process(jobs: Map[String, Option[ActorRef]]): Receive = {
//      case req: RunJobRequest => jobs.get(req.id) match {
//        case Some(ref) => log.warning(s"Request with ${req.id} was already handled, ignoring")
//        case None =>
//          val id = context.name + "_" + UUID.randomUUID().toString
//          log.info(s"Starting remote worker, $id for ${context.name}")
//          //TODO SOme(id) ???
//          spawnSettings.runner.runWorker(id, context, RunMode.ExclusiveContext(Some(id)))
//          regHub.waitRef(id, spawnSettings.timeout).map(remote => {
//          })
//        }
//    }
//
//  }
//
//  class DefaultStarter(
//    regHub: ActorRegHub,
//    af: ActorRefFactory
//  ) extends RemoteStarter {
//
//    def start(config: ContextConfig, spawnSettings: SpawnSettings): Future[ActorRef] = {
//      config.workerMode match {
//        case "shared" => shared(config, spawnSettings)
//        case "exclusive" =>
//        case x => Future.failed(throw new IllegalArgumentException(s"Unknown worker mode $x"))
//      }
//
//    }
//
//    private def shared(config: ContextConfig, spawnSettings: SpawnSettings): Future[ActorRef] = {
//      val id = config.name + "_" + UUID.randomUUID().toString
//      spawnSettings.runner.runWorker(id, config, RunMode.Shared)
//      regHub.waitRef(id, spawnSettings.timeout).map(ref => {
//
//      })
//    }
//  }
//
//  def apply(regHub: ActorRegHub)(implicit fa: ActorRefFactory): RemoteStarter = {
//
//  }
//
//
//  private def exclusive(config: ContextConfig, spawnSettings: SpawnSettings): Future[ActorRef] = {
//    val id = config.name + "_" + UUID.randomUUID().toString
//    //TODO
//    spawnSettings.runner.runWorker(id, config, RunMode.ExclusiveContext(Some(id)))
//  }
//
//}
