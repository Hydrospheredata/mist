package io.hydrosphere.mist

import java.io.File

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import io.hydrosphere.mist.jobs.JobFile
import io.hydrosphere.mist.utils.{Collections, ExternalJar, ExternalMethodArgument, Logger}

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
    ConfigFactory.parseFile(configFile)
  }
  
  def routes: Map[String, Any] = {
    val javaMap = config.root().unwrapped()
    Collections.asScalaRecursively(javaMap)
  }
  
  def info: Map[String, Map[String, Any]] = {
    routes.map {
      case (key: String, value: Map[String, Any]) =>
        if (JobFile.fileType(value("path").asInstanceOf[String]) == JobFile.FileType.Python) {
          key -> (Map("isPython" -> true) ++ value)
        } else {
          val jobFile = JobFile(value("path").asInstanceOf[String])
          if (!jobFile.exists) {
            throw new Exception(s"File ${value("path")} in `$key` not found")
          }

          val externalClass = ExternalJar(jobFile.file).getExternalClass(value("className").asInstanceOf[String])
          val inst = externalClass.getNewInstance

          def methodInfo(methodName: String): Map[String, Map[String, String]] = {
            try {
              Map(methodName -> inst.getMethod(methodName).arguments.flatMap { arg: ExternalMethodArgument =>
                Map(arg.name -> arg.tpe.toString)
              }.toMap[String, String]
              )
            } catch {
              case _: Throwable => Map.empty[String, Map[String, String]]
            }
          }

          val classInfo = Map(
            "isMLJob" -> externalClass.isMLJob,
            "isStreamingJob" -> externalClass.isStreamingJob,
            "isSqlJob" -> externalClass.isSqlJob,
            "isHiveJob" -> externalClass.isHiveJob
          )
          key -> (classInfo ++ value ++ methodInfo("execute") ++ methodInfo("train") ++ methodInfo("serve"))
        }
    }
  }

  def apply(route: String): RouteConfig = {
    new RouteConfig(route, config)
  }
}
