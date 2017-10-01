package io.hydrosphere.mist.master

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import io.hydrosphere.mist.master.ConfigUtils._
import io.hydrosphere.mist.master.data.ConfigRepr
import io.hydrosphere.mist.master.models.ContextConfig

import cats.syntax.option._

import scala.collection.JavaConversions._
import scala.concurrent.duration._

case class AsyncInterfaceConfig(
  host: String,
  port: Int,
  publishTopic: String,
  subscribeTopic: String
)

case class HostPortConfig(
  host: String,
  port: Int
)

object HostPortConfig {

  def apply(config: Config): HostPortConfig =
    HostPortConfig(config.getString("host"), config.getInt("port"))
}

case class HttpConfig(
  host: String,
  port: Int,
  uiPath: String
)

object HttpConfig {

  def apply(config: Config): HttpConfig =
    HttpConfig(config.getString("host"), config.getInt("port"), config.getString("ui"))
}


case class LogServiceConfig(
  host: String,
  port: Int,
  dumpDirectory: String
)

object LogServiceConfig {

  def apply(config: Config): LogServiceConfig = {
    LogServiceConfig(
      host = config.getString("host"),
      port = config.getInt("port"),
      dumpDirectory = config.getString("dump_directory")
    )
  }
}

object AsyncInterfaceConfig {

  def apply(config: Config): AsyncInterfaceConfig = {
    AsyncInterfaceConfig(
      host = config.getString("host"),
      port = config.getInt("port"),
      publishTopic = config.getString("publish-topic"),
      subscribeTopic = config.getString("subscribe-topic")
    )
  }

  def ifEnabled(config: Config): Option[AsyncInterfaceConfig] = {
    if (config.getBoolean("on"))
      AsyncInterfaceConfig(config).some
    else
      None
  }

}

case class WorkersSettingsConfig(
  runner: String,
  runnerInitTimeout: Duration,
  dockerHost: String,
  dockerPort: Int,
  cmd: String,
  cmdStop: String
)

object WorkersSettingsConfig {

  def apply(config: Config): WorkersSettingsConfig = {
    WorkersSettingsConfig(
      runner = config.getString("runner"),
      runnerInitTimeout = Duration(config.getString("runner-init-timeout")),
      dockerHost = config.getString("docker-host"),
      dockerPort = config.getInt("docker-port"),
      cmd = config.getString("cmd"),
      cmdStop = config.getString("cmdStop")
    )
  }

}

/**
  * Context settings that are preconfigured in main config
  */
case class ContextsSettings(
  default: ContextConfig,
  contexts: Map[String, ContextConfig]
)

object ContextsSettings {

  val Default = "default"

  def apply(config: Config): ContextsSettings = {
    val defaultCfg = config.getConfig("context-defaults")
    val default = ConfigRepr.ContextConfigRepr.fromConfig(Default, defaultCfg)

    val contextsCfg = config.getConfig("context")
    val contexts = contextsCfg.root().entrySet().filter(entry => {
      entry.getValue.valueType() == ConfigValueType.OBJECT
    }).map(entry => {
      val name = entry.getKey
      val cfg = contextsCfg.getConfig(name).withFallback(defaultCfg)
      name -> ConfigRepr.ContextConfigRepr.fromConfig(name, cfg)
    }).toMap

    ContextsSettings(default, contexts)
  }

}

case class SecurityConfig(
  keytab: String,
  principal: String,
  interval: FiniteDuration
)

object SecurityConfig {
  def apply(c: Config): SecurityConfig = {
    SecurityConfig(
      keytab = c.getString("keytab"),
      principal = c.getString("principal"),
      interval = c.getFiniteDuration("interval")
    )
  }

  def ifEnabled(c: Config): Option[SecurityConfig] = {
    if (c.getBoolean("enabled")) SecurityConfig(c).some else None
  }

}

case class MasterConfig(
  cluster: HostPortConfig,
  http: HttpConfig,
  mqtt: Option[AsyncInterfaceConfig],
  kafka: Option[AsyncInterfaceConfig],
  logs: LogServiceConfig,
  workers: WorkersSettingsConfig,
  contextsSettings: ContextsSettings,
  dbPath: String,
  contextsPath: String,
  endpointsPath: String,
  security: Option[SecurityConfig],
  srcConfigPath: String,
  jobsSavePath: String,
  artifactRepositoryPath: String,
  raw: Config
)

object MasterConfig {

  def load(filePath: String): MasterConfig = {
    val cfg = loadConfig(filePath)
    parse(filePath, cfg)
  }

  def loadConfig(filePath: String): Config = {
    val appConfig = ConfigFactory.parseResourcesAnySyntax("master.conf")
    val user = ConfigFactory.parseFile(new File(filePath))
    val properties = ConfigFactory.systemProperties()
    properties.withFallback(user.withFallback(appConfig)).resolve()
  }

  def parse(filePath: String, config: Config): MasterConfig = {
    val mist = config.getConfig("mist")
    MasterConfig(
      cluster = HostPortConfig(mist.getConfig("cluster")),
      http = HttpConfig(mist.getConfig("http")),
      mqtt = AsyncInterfaceConfig.ifEnabled(mist.getConfig("mqtt")),
      kafka = AsyncInterfaceConfig.ifEnabled(mist.getConfig("kafka")),
      logs = LogServiceConfig(mist.getConfig("log-service")),
      workers = WorkersSettingsConfig(mist.getConfig("workers")),
      contextsSettings = ContextsSettings(mist),
      dbPath = mist.getString("db.filepath"),
      contextsPath = mist.getString("contexts-store.path"),
      endpointsPath = mist.getString("endpoints-store.path"),
      jobsSavePath = mist.getString("jobs-resolver.save-path"),
      artifactRepositoryPath = mist.getString("artifact-repository.save-path"),
      security = SecurityConfig.ifEnabled(mist.getConfig("security")),
      srcConfigPath = filePath,
      raw = config
    )
  }

}

object ConfigUtils {

  implicit class ExtConfig(c: Config) {

    def getFiniteDuration(path: String): FiniteDuration =
      getScalaDuration(path) match {
        case f: FiniteDuration => f
        case _ => throw new IllegalArgumentException(s"Can not crate finite duration from $path")
      }

    def getScalaDuration(path: String): Duration = Duration(c.getString(path))
  }
}
