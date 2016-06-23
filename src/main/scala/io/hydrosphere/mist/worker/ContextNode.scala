package io.hydrosphere.mist.worker

import java.util.concurrent.Executors._

import akka.cluster.ClusterEvent._
import io.hydrosphere.mist.Messages.WorkerDidStart
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.{Job, JobConfiguration}

import akka.cluster.Cluster
import akka.actor.{Props, ActorLogging, Actor}

import io.hydrosphere.mist.{Constants, MistConfig}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Random}

class ContextNode(name: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/" + Constants.Actors.workerManagerName
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(name)

  override def preStart() {
    serverActor ! WorkerDidStart(name, cluster.selfAddress.toString)
    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
  }

  override def postStop() {
    cluster.unsubscribe(self)
  }


  override def receive: Receive = {
    case jobRequest: JobConfiguration =>
      println(s"[WORKER] received JobRequest: $jobRequest")
      val originalSender = sender

      lazy val job = Job(jobRequest, contextWrapper, self.path.name)

      val future: Future[Either[Map[String, Any], String]] = Future {
        println(s"${jobRequest.name}#${job.id} is running")
        job.run()
      }(executionContext)
      future
        // TODO: recovery
        .andThen {
          case Success(result: Either[Map[String, Any], String]) => originalSender ! result
          case Failure(error: Throwable) => originalSender ! Right(error.toString)
        }(ExecutionContext.global)


    case MemberExited(member) =>
      if (member.address == cluster.selfAddress) {
        cluster.system.shutdown()
      }
  }
}

object ContextNode {
  def props(name: String) = Props(classOf[ContextNode], name)
}
