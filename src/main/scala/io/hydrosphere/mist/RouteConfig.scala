package io.hydrosphere.mist

import java.io.File

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import io.hydrosphere.mist.jobs.JobFile
import io.hydrosphere.mist.master.JobDefinition
import io.hydrosphere.mist.utils._

import scala.util.{Failure, Success, Try}

private[mist] class RouteConfig(route: String, config: Config) {

  lazy val routeConfig: Config = try {
    config.getConfig(route)
  } catch {
    case _: ConfigException.Missing => throw new RouteConfig.RouteNotFoundError(s"Route configuration /$route not found")
  }

  private def getString(path: String): String = {
    try {
      routeConfig.getString(path)
    } catch {
      case _: ConfigException.Missing => throw new RouteConfig.ConfigSettingsNotFoundError(s"Configuration for $path in $route not found")
    }
  }

  def path: String = getString("path")

  def className: String = getString("className")

  def namespace: String = getString("namespace")
}

private[mist] object RouteConfig extends Logger {

  class RouteNotFoundError(message: String) extends Exception
  class ConfigSettingsNotFoundError(message: String) extends Exception
  class RouterConfigurationMissingError(message: String) extends Exception
  
  private def config = {
    val configFile = try {
      new File(MistConfig.Http.routerConfigPath)
    } catch {
      case _: ConfigException.Missing => throw new RouterConfigurationMissingError(s"Router configuration file is not defined")
    }
    if (!configFile.exists()) {
      logger.error(s"${MistConfig.Http.routerConfigPath} does not exists")
      throw new RouterConfigurationMissingError(s"${MistConfig.Http.routerConfigPath} does not exists")
    }
    ConfigFactory.parseFile(configFile).resolve()
  }
  
  def routes: Map[String, Any] = {
    val javaMap = config.root().unwrapped()
    Collections.asScalaRecursively(javaMap)
  }

  //TODO: move it out of here
  private def methodInfo(inst: ExternalInstance, methodName: String): Map[String, Map[String, String]] = {
    try {
      Map(methodName -> inst.getMethod(methodName).arguments.flatMap { arg: ExternalMethodArgument =>
        Map(arg.name -> arg.tpe.toString)
      }.toMap[String, String]
      )
    } catch {
      case _: Throwable => Map.empty[String, Map[String, String]]
    }
  }

  //TODO: move it out of here
  private def buildJobInfo(job: JobDefinition, file: JobFile): Map[String, Any] = {
    val fileType = JobFile.fileType(job.path)
    fileType match {
      case JobFile.FileType.Python =>
        Map("isPython" -> true)

      case JobFile.FileType.Jar =>
        val externalClass = ExternalJar(file.file).getExternalClass(job.className)
        val inst = externalClass.getNewInstance
        val classInfo = Map(
          "isMLJob" -> externalClass.isMLJob,
          "isStreamingJob" -> externalClass.isStreamingJob,
          "isSqlJob" -> externalClass.isSqlJob,
          "isHiveJob" -> externalClass.isHiveJob
        )
        classInfo ++ methodInfo(inst, "execute") ++ methodInfo(inst, "train") ++ methodInfo(inst, "serve")
    }
  }

  private def buildRoutesInfo(config: Config): Map[String, Map[String, Any]] = {
    val definitions = JobDefinition.parseConfig(config)
    definitions.map(_.flatMap(job => Try {
      val jobFile = JobFile(job.path)
      //TODO: call file here, to throw exception if file not nound
      jobFile.file
      val info = buildJobInfo(job, jobFile)
      job.name -> info
    })).flatMap({
      case Success(info) => Some(info)
      case Failure(e) =>
        logger.error("Can not build job info", e)
        None
    }).toMap
  }

  def info: Map[String, Map[String, Any]] =
    buildRoutesInfo(config)

  def apply(route: String): RouteConfig = {
    new RouteConfig(route, config)
  }
}
