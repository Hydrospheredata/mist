package io.hydrosphere.mist.cli

import java.util.concurrent.Executors._

import akka.actor.{Actor, ActorRef, Address}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.Messages._
import io.hydrosphere.mist.jobs.JobDetails
import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService}
import scala.util.{Failure, Random, Success}

class CLINode extends Actor {

  val executionContext: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.cliResponderName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress: Address = cluster.selfAddress
  
  private implicit val timeout = Timeout.durationToTimeout(Constants.CLI.timeoutDuration) 

  override def preStart(): Unit = {
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  def cliResponder[A](el: A, originalSender: akka.actor.ActorRef): Unit = {
    val future = serverActor ? el
    future.andThen {
      case Success(result: Map[_, _]) => originalSender ! result
      case Success(result: String) => originalSender ! result
      case Success(result: List[JobDetails]) => originalSender ! result.map {
        job: JobDetails => JobDescription(job)
      }
      case Success(result: List[Any]) => originalSender ! result
      case Failure(error: Throwable) => originalSender ! error
    }(ExecutionContext.global)
  }

  override def receive: Receive = {
    case StopWorker(workerIdentifier) =>
      serverActor ! RemoveContext(workerIdentifier)
      sender ! s"Worker $workerIdentifier is scheduled for shutdown."

    case message: StopJob =>
      cliResponder(message, sender)

    case StopAllContexts() =>
      serverActor ! StopAllContexts()
      sender ! Constants.CLI.stopAllWorkers

    case ListJobs() =>
      serverActor ! ListJobs()
      context become result(sender)

    case ListWorkers() =>
      serverActor ! ListWorkers()
      context become result(sender)

    case ListRoutes() =>
      serverActor ! ListRoutes()
      context become result(sender)

    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }

    case MemberRemoved(member, _) =>
      if (member.address == cluster.selfAddress) {
        cluster.down(nodeAddress)
        sys.exit(0)
      }
  }
  
  def result(originalSender: ActorRef): Receive = {
    case response: List[_] =>
      originalSender ! response
    case response: String =>
      originalSender ! response
  }
}
