package io.hydrosphere.mist

import akka.actor.{ActorSystem, Props}
import io.hydrosphere.mist.utils.JsonFormatSupport
import io.hydrosphere.mist.worker.JobRunnerNode
import spray.json.pimpString

private[mist] object JobEntryPoint extends App with Logger with JsonFormatSupport{

  implicit val system = ActorSystem("mist", MistConfig.Akka.Worker.settings)

  val contextNode =
    if (args.length == 5) {
      system.actorOf(
        Props(
          new JobRunnerNode(
            args(0),
            args(1),
            args(2),
            args(3),
            args(4).toString.parseJson.convertTo[Map[String, Any]])),
        name = "JobStarter")
    } else if (args.length == 4) {
      system.actorOf(
        Props(
          new JobRunnerNode(
            args(0),
            args(1),
            args(2),
            args(3),
            Map().empty)),
        name = "JobStarter")
    } else if (args.length == 1 || args.length == 2) {
      val jobRoute = args(0)
      val jobRequestParams = if (args.length == 2) {
        args(1).toString.parseJson.convertTo[Map[String, Any]]
      } else {
        Map.empty[String, Any]
      }
      try {
        val config = RouteConfig(jobRoute)
        system.actorOf(
          Props(
            new JobRunnerNode(
              config.path,
              config.className,
              config.namespace,
              "",
              jobRequestParams)),
          name = "JobStarter")
      } catch {
        case exc: RouteConfig.RouteNotFoundError =>
          logger.error(s"Route $jobRoute not found")
          logger.error(exc.toString)
          System.exit(1)
      }
    }
}
