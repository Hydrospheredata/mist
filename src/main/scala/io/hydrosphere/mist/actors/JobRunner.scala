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

private[mist] class JobRunner extends Actor {

  // Thread context for parallel running jobs
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(MistConfig.Settings.threadNumber))

  // Actor which is creates spark contexts
  lazy val contextManager: ActorRef = context.actorOf(Props[ContextManager], name = Constants.Actors.contextManagerName)

  override def receive: Receive = {
    case configuration: JobConfiguration =>
      val originalSender = sender

      // Time of spark context creating is definitely less than one day
      implicit val timeout = Timeout(1.day)

      // Request spark context creating
      val contextFuture = contextManager ? CreateContext(configuration.name)

      contextFuture.flatMap {
        case contextWrapper: ContextWrapper =>

          lazy val job = Job(configuration, contextWrapper, self.path.name)

          lazy val jobRepository = {
            MistConfig.Recovery.recoveryOn match {
              case true => RecoveryJobRepository
              case _ => InMemoryJobRepository
            }
          }
          val future: Future[Either[Map[String, Any], String]] = Future {
            jobRepository.add(job)
            println(s"${configuration.name}#${job.id} is running")
            job.run()
          }(executionContext)
          future
            .andThen {
              case _ => {
                if (MistConfig.Contexts.isDisposable(configuration.name)) {
                  contextManager ! RemoveContext(contextWrapper)
                }
                jobRepository.remove(job)
            }
            }(ExecutionContext.global)
            .andThen {
              case Success(result: Either[Map[String, Any], String]) => originalSender ! result
              case Failure(error: Throwable) => originalSender ! Right(error.toString)
            }(ExecutionContext.global)
      }(ExecutionContext.global)
  }
}
