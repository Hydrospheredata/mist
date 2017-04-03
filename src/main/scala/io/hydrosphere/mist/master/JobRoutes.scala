package io.hydrosphere.mist.master

import com.typesafe.config.Config
import io.hydrosphere.mist.jobs.{JobDefinition, JobInfo}
import io.hydrosphere.mist.utils.Logger

import scala.util.{Failure, Success}

class JobRoutes(config: Config) extends Logger {

  def listDefinition(): Seq[JobDefinition] = {
    JobDefinition.parseConfig(config).flatMap({
      case Success(d) => Some(d)
      case Failure(e) =>
        logger.error("Invalid route configuration", e)
        None
    })
  }

  def listInfos(): Seq[JobInfo] = listDefinition().flatMap(loadInfo)

  def getDefinition(id: String): Option[JobDefinition] =
    listDefinition().find(_.name == id)

  def getInfo(id: String): Option[JobInfo] =
    getDefinition(id).flatMap(loadInfo)

  private def loadInfo(definition: JobDefinition): Option[JobInfo] = {
    JobInfo.load(definition) match {
      case Success(info) => Some(info)
      case Failure(e) =>
        logger.error(s"Job's loading failed for ${definition.name}", e)
        None
    }
  }
}
