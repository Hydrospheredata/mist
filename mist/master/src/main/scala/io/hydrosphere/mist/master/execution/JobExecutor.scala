package io.hydrosphere.mist.master.execution

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef}
import io.hydrosphere.mist.core.CommonData.CancelJobRequest
import io.hydrosphere.mist.master.JobResult
import io.hydrosphere.mist.master.models.RunMode
import io.hydrosphere.mist.utils.akka.ActorRegHub

import scala.collection.immutable.Queue
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success}
//import akka.actor.{Actor, ActorLogging}
import io.hydrosphere.mist.core.CommonData.RunJobRequest
import io.hydrosphere.mist.master.models.ContextConfig


//object Execution {
//
//  sealed trait ExecutorEvent
//  case class ExecutorReady(id: String, ref: ActorRef) extends ExecutorEvent
//  case class ExecutorFailed(id: String) extends ExecutorEvent
//

//  def emptyData(context: ContextConfig, spawnSettings: SpawnSettings): ExecutionData = {
//    val st = new ProcessState[JobStatus, String](
//      queued = Queue.empty,
//      inProgress = Map.empty,
//      _.request.id
//    )
//    ExecutionData(context, st, spawnSettings)
//  }
//
//  trait Utility {
//    def newJob(req: RunJobRequest): JobStatus =
//      JobStatus(req, ExecState.Queued, Promise[JobResult])
//
//  }
//
//  trait ExecutorStarter {
//    def start(config: ContextConfig): Future[ActorRef]
//  }
//
//  class Basic(
//    data: ExecutionData,
//    starter: ExecutorStarter
//  ) extends Actor with ActorLogging with Utility {
//
//    override def receive: Receive = initial(data)
//
//    def initial(data: ExecutionData): Receive = {
//      case req: RunJobRequest =>
//        val js = newJob(req)
//        val nextProcess = data.processState.enqueue(js)
//        val nextData = data.copy(processState = nextProcess)

//        val id = data.config.name + "_" + UUID.randomUUID().toString
//        data.spawnSettings.runner.runWorker(id, data.config, RunMode.Shared)
//        regHub.waitRef(id, data.spawnSettings.timeout).onComplete {
//          case Success(ref) => self ! ExecutorReady(id, ref)
//          case Failure(e) => self ! ExecutorFailed(id)
//        }
//        starter.start(data.config).onComplete {
//          case Success(ref) => self ! ExecutorReady(id, ref)
//          case Failure(e) => self ! ExecutorFailed(id)
//        }
//        context.become(waitExecutor(id, nextData))
//    }
//
//    def waitExecutor(executorId: String, data: ExecutionData): Receive = {
//      case ExecutorReady(id, ref) if id == executorId =>
//        context.become(withExecutor(ref, data))
//      case ExecutorFailed(id) if executorId == id =>
//        //TODO ???
//      case req: RunJobRequest =>
//        val js = newJob(req)
//        val nextProcess = data.processState.enqueue(js)
//        val nextData = data.copy(processState = nextProcess)
//        context.become(waitExecutor(executorId, nextData))
//
//      case CancelJobRequest(id) =>
//
//
//    }
//
//    def withExecutor(ref: ActorRef, data: ExecutionData): Receive = {
//
//    }
//
//
//  }



//}
