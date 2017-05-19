package io.hydrosphere.mist.master

import java.io.File
import java.nio.file.Paths

import com.typesafe.config.{ConfigValueFactory, Config, ConfigFactory}
import io.hydrosphere.mist.jobs.{JobDefinition, JobInfo}
import io.hydrosphere.mist.utils.Logger

import scala.util.{Failure, Success}

/**
  * Job routes information provider (based on router config an job internal info)
 *
  * @param path - path to router config
  */
class JobRoutes(path: String) extends Logger {

  private def loadConfig(): Config = {
    val directory = Paths.get(path).getParent
    val file = new File(path)
    ConfigFactory
      .parseFile(file)
      .withValue("location", ConfigValueFactory.fromAnyRef(directory.toString))
      .resolve()
  }

  def listDefinition(): Seq[JobDefinition] = {
    try {
      val parsed = JobDefinition.parseConfig(loadConfig())
      parsed
        .collect({ case Failure(e) => e })
        .foreach(e => logger.error("Invalid route configuration", e))

      parsed.collect({ case Success(x) => x })
        .map(x => {println(x.path); x})
    } catch {
      case e: Throwable =>
        logger.error("Router configuration reading failed", e)
        Seq.empty
    }
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
