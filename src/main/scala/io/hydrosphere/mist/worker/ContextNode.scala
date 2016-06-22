package io.hydrosphere.mist.worker

import java.util.concurrent.Executors._

import akka.cluster.ClusterEvent.{UnreachableMember, MemberEvent, InitialStateAsEvents}
import akka.pattern.ask
import io.hydrosphere.mist.actors.JobRunner
import io.hydrosphere.mist.actors.tools.Messages.{RemoveContext, WorkerDidStart, CreateContext}
import io.hydrosphere.mist.contexts.ContextBuilder
import io.hydrosphere.mist.jobs.{InMemoryJobRepository, RecoveryJobRepository, Job, JobConfiguration}

import collection.JavaConversions._

import akka.cluster.Cluster
import akka.actor.{Props, ActorLogging, Actor}

import io.hydrosphere.mist.MistConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Random}

class ContextNode(name: String) extends Actor with ActorLogging{

  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  private val cluster = Cluster(context.system)

  private val serverAddress = Random.shuffle[String, List](MistConfig.Akka.Worker.serverList).head + "/user/clusterMist"
  private val serverActor = cluster.system.actorSelection(serverAddress)

  val nodeAddress = cluster.selfAddress

  lazy val contextWrapper = ContextBuilder.namedSparkContext(name)
  lazy val jobRunnerActor = context.system.actorOf(JobRunner.props(contextWrapper))

  override def preStart() {
//    cluster.subscribe(self, InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])
    serverActor ! WorkerDidStart(name, cluster.selfAddress.toString)
  }

  override def postStop() {
//    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case jobRequest: JobConfiguration =>
      val originalSender = sender

      lazy val job = Job(jobRequest, contextWrapper, self.path.name)

//      lazy val jobRepository = {
//        MistConfig.Recovery.recoveryOn match {
//          case true => RecoveryJobRepository
//          case _ => InMemoryJobRepository
//        }
//      }
      val future: Future[Either[Map[String, Any], String]] = Future {
//        jobRepository.add(job)
        println(s"${jobRequest.name}#${job.id} is running")
        job.run()
      }(executionContext)
      future
        // TODO: recovery
        // TODO: disposable context
        .andThen {
          case Success(result: Either[Map[String, Any], String]) => originalSender ! result
          case Failure(error: Throwable) => originalSender ! Right(error.toString)
        }(ExecutionContext.global)
  }
}

object ContextNode {
  def props(name: String) = Props(classOf[ContextNode], name)
}
