package io.hydrosphere.mist.worker

import java.util.concurrent.Executors.newFixedThreadPool

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.FullJobConfiguration
import akka.cluster.Cluster
import akka.actor.{Actor, ActorLogging, Props}
import io.hydrosphere.mist.jobs.runners.Runner
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success}

class ContextNode(namespace: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(namespace)

  override def preStart(): Unit = {
    serverActor ! WorkerDidStart(namespace, cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case jobRequest: FullJobConfiguration =>
      log.info(s"[WORKER] received JobRequest: $jobRequest")
      val originalSender = sender

      lazy val runner = Runner(jobRequest, contextWrapper)

      val future: Future[Either[Map[String, Any], String]] = Future {
        serverActor ! AddJobToRecovery(runner.id, runner.configuration)
        log.info(s"${jobRequest.namespace}#${runner.id} is running")
        runner.run()
      }(executionContext)
      future
        .recover {
         case e: Throwable => originalSender ! Right(e.toString)
        }(ExecutionContext.global)
        .andThen {
        case _ =>
          serverActor ! RemoveJobFromRecovery(runner.id)
        }(ExecutionContext.global)
        .andThen {
          case Success(result: Either[Map[String, Any], String]) => originalSender ! result
          case Failure(error: Throwable) => originalSender ! Right(error.toString)
        }(ExecutionContext.global)

    case StartInfinityJob(jobConfiguration) =>
      log.info(s"[WORKER] received StreamJobRequest: $jobConfiguration")
      val originalSender = sender

      lazy val runner = Runner(jobConfiguration, contextWrapper)

      val future: Future[Either[Map[String, Any], String]] = Future {
        log.info(s"${jobConfiguration.name}#${runner.id} is running")
        runner.run()
      }(executionContext)
      future
        .recover {
          case e: Throwable => log.error(e, s"[WORKER]  ${jobConfiguration.name}#${runner.id}" + e.getMessage)
        }(ExecutionContext.global)
        .andThen {
          case _ =>
            originalSender ! RemoveContext(name)
        }(ExecutionContext.global)
        .andThen {
          case Success(_) => {
            cluster.system.shutdown()
          }
          case Failure(error: Throwable) => log.error(error, s"[WORKER]  ${jobConfiguration.name}#${runner.id}" + error.getMessage)
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
  def props(namespace: String): Props = Props(classOf[ContextNode], namespace)
}
