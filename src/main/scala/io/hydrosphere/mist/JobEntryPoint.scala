package io.hydrosphere.mist

import java.io.File

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import io.hydrosphere.mist.jobs.{Action, JobExecutionParams}
import io.hydrosphere.mist.master.JobRoutes
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.TypeAlias.JobParameters
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.worker.JobRunnerNode
import spray.json.pimpString

import scala.util.{Failure, Success, Try}

//TODO: terminate system after running job
object JobEntryPoint extends App with Logger with JobConfigurationJsonSerialization{

  if (args.length < 2) {
    val msg =
      s"""Not enough arguments for starting job:
         |- ACTION - type of job action (execute, train, serve)
         |- JOB_ID - job route id in config
         |- ARGUMENTS - execution arguments (optional)
         |- EXTERNAL_ID - execution id (optional)
       """.stripMargin

    sys.error(msg)
  }

  val action = Action(args(0))
  val jobId = args(1)

  val routesFile = new File(MistConfig.Http.routerConfigPath)
  val routeConfig = ConfigFactory.parseFile(routesFile).resolve()
  val jobRoutes = new JobRoutes(routeConfig)

  val jobDefinition = jobRoutes.getDefinition(jobId) match {
    case Some(d) => d
    case None =>
      sys.error(s"Job with id: $jobId not found in $routesFile")
  }

  val parameters = Try {
    optArg(3)
      .map(_.parseJson.convertTo[JobParameters])
      .getOrElse(JobParameters.empty)
  } match {
    case Success(p) => p
    case Failure(e) =>
      println("Can not parse job arguments")
      e.printStackTrace()
      sys.exit(1)
  }

  val externalId = optArg(4)

  val execParams = JobExecutionParams.fromDefinition(jobDefinition, action, parameters, externalId)

  val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)
  val runner = system.actorOf(JobRunnerNode.props(execParams))

  def optArg(index: Int): Option[String] =
    if (args.length <= index)
      None
    else
      Option(args(index))

}

