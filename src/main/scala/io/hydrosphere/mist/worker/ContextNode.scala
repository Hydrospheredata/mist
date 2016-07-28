package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.{AddJobToRecovery, RemoveJobFromRecovery, WorkerDidStart}
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.{Job, JobConfiguration}
import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props}
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

class ContextNode(name: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(name)

  override def preStart(): Unit = {
    serverActor ! WorkerDidStart(name, cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case jobRequest: JobConfiguration =>
      log.info(s"[WORKER] received JobRequest: $jobRequest")
      val originalSender = sender

      lazy val job = Job(jobRequest, contextWrapper, self.path.name)

      val future: Future[Either[Map[String, Any], String]] = Future {
        serverActor ! AddJobToRecovery(job.id, job.configuration)
        log.info(s"${jobRequest.name}#${job.id} is running")
        job.run()
      }(executionContext)
      future
        .recover {
         case e: Throwable => originalSender ! Right(e.toString)
        }(ExecutionContext.global)
        .andThen {
        case _ =>
          serverActor ! RemoveJobFromRecovery(job.id)
        }(ExecutionContext.global)
        .andThen {
          case Success(result: Either[Map[String, Any], String]) => originalSender ! result
          case Failure(error: Throwable) => originalSender ! Right(error.toString)
        }(ExecutionContext.global)

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, prevStatus) =>
      if (member.address == cluster.selfAddress) {
        sys.exit(0)
      }
  }
}

object ContextNode {
  def props(name: String): Props = Props(classOf[ContextNode], name)
}
