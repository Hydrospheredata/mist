package io.hydrosphere.mist

import akka.actor.ActorSystem
import io.hydrosphere.mist.jobs.FullJobConfigurationBuilder
import io.hydrosphere.mist.utils.Logger
import io.hydrosphere.mist.utils.json.JobConfigurationJsonSerialization
import io.hydrosphere.mist.worker.JobRunnerNode
import spray.json.pimpString

private[mist] object JobEntryPoint extends App with Logger with JobConfigurationJsonSerialization{

  implicit val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)
  val contextNode =
    if (args.length == 6) {
      val jobConfiguration = FullJobConfigurationBuilder()
        .setPath(args(1))
        .setClassName(args(2))
        .setNamespace(args(3))
        .setExternalId(Some(args(4)))
        .setParameters(args(5).toString.parseJson.convertTo[Map[String, Any]])
        .build()
      system.actorOf(JobRunnerNode.props(jobConfiguration))
    } else if (args.length == 5) {
      val jobConfiguration = FullJobConfigurationBuilder()
          .setPath(args(1))
          .setClassName(args(2))
          .setNamespace(args(3))
          .setExternalId(Some(args(4)))
          .setParameters(Map.empty[String, Any])
          .build()
      system.actorOf(JobRunnerNode.props(jobConfiguration))
    } else if (args.length == 2 || args.length == 3 || args.length == 4) {
      val jobRoute = args(1)
      val jobRequestParams = if (args.length == 3) {
        args(2).toString.parseJson.convertTo[Map[String, Any]]
      } else {
        Map.empty[String, Any]
      }
      val externalId = if (args.length == 4) {
        args(3).toString
      } else {
        null
      }

      try {
        val jobConfiguration = FullJobConfigurationBuilder()
          .fromRouter(jobRoute, jobRequestParams, Some(externalId))
          .setServing(args(0) == "serve")
          .setTraining(args(0) == "train")
          .build()
        system.actorOf(JobRunnerNode.props(jobConfiguration))
      } catch {
        case exc: RouteConfig.RouteNotFoundError =>
          logger.error(s"Route $jobRoute not found")
          logger.error(exc.toString)
          System.exit(1)
        case exc: RouteConfig.RouterConfigurationMissingError =>
          logger.error(exc.toString)
          logger.error(exc.getMessage)
          System.exit(1)
      }
    }
}
