package com.provectus.lymph.actors

import java.util.concurrent.Executors._

import akka.actor.{Props, ActorRef, Actor}
import akka.pattern.ask
import akka.util.Timeout
import com.provectus.lymph.{Constants, LymphConfig}
import com.provectus.lymph.actors.tools.Messages.{RemoveContext, CreateContext}
import com.provectus.lymph.jobs.{InMemoryJobRepository, Job, JobPy, JobConfiguration}
import com.provectus.lymph.contexts._

import scala.concurrent.{Future, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

private[lymph] class JobRunner extends Actor {

  // Thread context for parallel running jobs
  val executionContext = ExecutionContext.fromExecutorService(newFixedThreadPool(LymphConfig.Settings.threadNumber))

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
        case contextWrapper: ContextWrapper => {

          lazy val job = new Job(configuration, contextWrapper)
          if(configuration.jarPath.nonEmpty) {
            println(s"${configuration.name}#${job.id} is running")
            InMemoryJobRepository.add(job)
          }

          lazy val jobpy = new JobPy(configuration, contextWrapper)
          if(configuration.pyPath.nonEmpty) {
            println(s"${configuration.name}#${jobpy.id} is running")
          }
          //TODO InMemoryJobRepository.add(jobpy)

          val future: Future[Either[Map[String, Any], String]] = Future {
            if(configuration.jarPath.nonEmpty)
              job.run()
            else if(configuration.pyPath.nonEmpty)
              jobpy.run()
            else
              throw new Exception("Error patch file, jar or python")
          }(executionContext)
          future
            .andThen {
              case _ =>
                if (LymphConfig.Contexts.isDisposable(configuration.name)) {
                  contextManager ! RemoveContext(contextWrapper)
                }
            }(ExecutionContext.global)
            .andThen {
              case Success(result: Either[Map[String, Any], String]) => originalSender ! result
              case Failure(error: Throwable) => originalSender ! Right(error.toString)
            }(ExecutionContext.global)
        }
      }(ExecutionContext.global)
  }
}
