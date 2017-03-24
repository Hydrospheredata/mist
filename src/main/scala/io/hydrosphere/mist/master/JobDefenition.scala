package io.hydrosphere.mist.master

import com.typesafe.config.Config

import scala.util._

/**
  * Configuration for mist job
  *
  * @param path - path to jar or py
  * @param className - entryPoint
  * @param nameSpace - job context namespace
  */
case class JobConfiguration(
  path: String,
  className: String,
  nameSpace: String
)

/**
  * Job name and config params
  */
case class JobDefinition(
  name: String,
  path: String,
  className: String,
  nameSpace: String
)

object JobConfiguration {

  def fromConfig(config: Config): Try[JobConfiguration] = {
    val extracted = Try {
      Seq(
        config.getString("path"),
        config.getString("className"),
        config.getString("namespace")
      ).flatMap(Option(_))
    }
    extracted.flatMap({
      case path :: className :: namespace :: Nil =>
        Success(JobConfiguration(path, className, namespace))
      case _ =>
        val msg = s"Parse job configuration failed, invalid config $config"
        Failure(new IllegalArgumentException(msg))
    })
  }
}

object JobDefinition {

  import scala.collection.JavaConversions._

  def apply(name: String, configuration: JobConfiguration): JobDefinition =
    JobDefinition(
      name,
      configuration.path,
      configuration.className,
      configuration.nameSpace
    )

  def parseConfig(config: Config): Seq[Try[JobDefinition]] = {
    config.root().keySet()
      .map(name => {
        val part = config.getConfig(name)
        val parsed = JobConfiguration.fromConfig(part)
        parsed.map(c => JobDefinition(name, c))
      }).toList
  }

}

