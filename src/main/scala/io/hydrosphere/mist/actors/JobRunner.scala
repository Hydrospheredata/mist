package io.hydrosphere.mist.actors

import java.util.concurrent.Executors._

import akka.actor.{Props, ActorRef, Actor}
import akka.pattern.ask
import akka.util.Timeout
import io.hydrosphere.mist.{Constants, MistConfig}
import io.hydrosphere.mist.actors.tools.Messages.{RemoveContext, CreateContext}
import io.hydrosphere.mist.jobs.{InMemoryJobRepository, RecoveryJobRepository, JobConfiguration, Job}
import io.hydrosphere.mist.contexts._

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[mist] class JobRunner(contextWrapper: ContextWrapper) extends Actor {

  // Thread context for parallel running jobs
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  // Actor which is creates spark contexts
  lazy val contextManager: ActorRef = context.actorOf(Props[WorkerManager], name = Constants.Actors.workerManagerName)

  override def receive: Receive = {
    case configuration: JobConfiguration =>

  }
}

object JobRunner {
  def props(contextWrapper: ContextWrapper) = Props(classOf[JobRunner], contextWrapper)
}