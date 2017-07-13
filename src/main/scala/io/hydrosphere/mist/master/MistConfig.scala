package io.hydrosphere.mist.master

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import scala.collection.JavaConversions._
import scala.concurrent.duration.{Duration, FiniteDuration}
import ConfigUtils._
import io.hydrosphere.mist.master.data.contexts.ContextConfig

case class AsyncInterfaceConfig(
  isOn: Boolean,
  host: String,
  port: Int,
  publishTopic: String,
  subscribeTopic: String
)

case class EndpointConfig(
  host: String,
  port: Int
)

object EndpointConfig {

  def apply(config: Config): EndpointConfig =
    EndpointConfig(config.getString("host"), config.getInt("port"))

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
      isOn = config.getBoolean("on"),
      host = config.getString("host"),
      port = config.getInt("port"),
      publishTopic = config.getString("publish-topic"),
      subscribeTopic = config.getString("subscribe-topic")
    )
  }
}

case class WorkersSettingsConfig(
  runner: String,
  dockerHost: String,
  dockerPort: Int,
  cmd: String,
  cmdStop: String
)

object WorkersSettingsConfig {

  def apply(config: Config): WorkersSettingsConfig = {
    WorkersSettingsConfig(
      runner = config.getString("runner"),
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
    val default = ContextConfig.fromConfig("default", defaultCfg)

    val contextsCfg = config.getConfig("context")
    val contexts = contextsCfg.entrySet().filter(entry => {
      entry.getValue.valueType() == ConfigValueType.OBJECT
    }).map(entry => {
      val name = entry.getKey
      val cfg = contextsCfg.getConfig(name).withFallback(defaultCfg)
      name -> ContextConfig.fromConfig(name, cfg)
    }).toMap

    ContextsSettings(default, contexts)
  }

}

case class SecurityConfig(
  enabled: Boolean,
  keytab: String,
  principal: String,
  interval: FiniteDuration
)

object SecurityConfig {
  def apply(c: Config): SecurityConfig = {
    SecurityConfig(
      enabled = c.getBoolean("enabled"),
      keytab = c.getString("keytab"),
      principal = c.getString("principal"),
      interval = c.getFiniteDuration("interval")
    )
  }
}

case class MasterConfig(
  cluster: EndpointConfig,
  http: EndpointConfig,
  mqtt: AsyncInterfaceConfig,
  kafka: AsyncInterfaceConfig,
  logs: LogServiceConfig,
  workers: WorkersSettingsConfig,
  contextsSettings: ContextsSettings,
  dbPath: String,
  contextsPath: String,
  security: SecurityConfig,
  raw: Config
)

object MasterConfig {

  def load(filePath: String): MasterConfig = {
    val default = ConfigFactory.load("master")
    val user = ConfigFactory.parseFile(new File(filePath))
    val cfg = user.withFallback(default).resolve()
    parse(cfg)
  }

  def parse(config: Config): MasterConfig = {
    val mist = config.getConfig("mist")
    MasterConfig(
      cluster = EndpointConfig(mist.getConfig("cluster")),
      http = EndpointConfig(mist.getConfig("http")),
      mqtt = AsyncInterfaceConfig(mist.getConfig("mqtt")),
      kafka = AsyncInterfaceConfig(mist.getConfig("kafka")),
      logs = LogServiceConfig(mist.getConfig("log-service")),
      workers = WorkersSettingsConfig(mist.getConfig("workers")),
      contextsSettings = ContextsSettings(mist),
      dbPath = mist.getString("db.filepath"),
      contextsPath = mist.getString("contexts-store.path"),
      security = SecurityConfig(mist.getConfig("security")),
      raw = config
    )
  }

}

case class WorkerConfig(
  logs: LogServiceConfig,
  contextsSettings: ContextsSettings,
  raw: Config
)

object WorkerConfig {

  def load(filePath: String): WorkerConfig = {
    val default = ConfigFactory.load("mist")
    val user = ConfigFactory.parseFile(new File(filePath))
    val cfg = user.withFallback(default).resolve()
    parse(cfg)
  }

  def parse(config: Config): WorkerConfig = {
    val mist = config.getConfig("mist")
    WorkerConfig(
      logs = LogServiceConfig(mist.getConfig("log-service")),
      contextsSettings = ContextsSettings(mist),
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
