package io.hydrosphere.mist

import java.io.File

import com.typesafe.config.{Config, ConfigException, ConfigFactory}

private[mist] class RouteConfig(route: String, config: Config) {

  lazy val routeConfig = try {
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

private[mist] object RouteConfig {

  class RouteNotFoundError(message: String) extends Exception
  class ConfigSettingsNotFoundError(message: String) extends Exception
  class RouterConfigurationMissingError(message: String) extends Exception

  def apply(route: String): RouteConfig = {
    val configFile = try {
      new File(MistConfig.HTTP.routerConfigPath)
    } catch {
      case _: ConfigException.Missing => throw new RouterConfigurationMissingError(s"Router configuration file is not defined")
    }
    if (!configFile.exists()) {
      throw new RouterConfigurationMissingError(s"${MistConfig.HTTP.routerConfigPath} does not exists")
    }
    val config = ConfigFactory.parseFile(configFile)
    new RouteConfig(route, config)
  }
}