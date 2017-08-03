package io.hydrosphere.mist.master

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import scala.collection.JavaConversions._
import scala.concurrent.duration._

import ConfigUtils._

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
      isOn = config.getBoolean("on"),
      host = config.getString("host"),
      port = config.getInt("port"),
      publishTopic = config.getString("publish-topic"),
      subscribeTopic = config.getString("subscribe-topic")
    )
  }

  val disabled: AsyncInterfaceConfig = AsyncInterfaceConfig(isOn = false, "", 0, "", "")
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

case class ContextConfig(
  name: String,
  sparkConf: Map[String, String],
  downtime: Duration,
  maxJobs: Int,
  precreated: Boolean,
  runOptions: String,
  streamingDuration: Duration
)

object ContextConfig {

  def apply(name: String, config: Config): ContextConfig = {
    ContextConfig(
      name = name,
      sparkConf = config.getConfig("spark-conf").entrySet()
        .filter(entry => entry.getValue.valueType() == ConfigValueType.STRING)
        .map(entry => entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String])
        .toMap,
      downtime = Duration(config.getString("downtime")),
      maxJobs = config.getInt("max-parallel-jobs"),
      precreated = config.getBoolean("precreated"),
      runOptions = config.getString("run-options"),
      streamingDuration = Duration(config.getString("streaming-duration"))
    )
  }
}

case class ContextsSettings(
  default: ContextConfig,
  contexts: Map[String, ContextConfig]
) {

  def precreated: Seq[ContextConfig] = contexts.values.filter(_.precreated).toSeq

  def configFor(name: String): ContextConfig = contexts.getOrElse(name, default)

  def getAll: Seq[ContextConfig] = default +: contexts.values.toList
}

object ContextsSettings {

  val Default = "default"

  def apply(config: Config): ContextsSettings = {
    val defaultCfg = config.getConfig("context-defaults")
    val default = ContextConfig("default", defaultCfg)

    val contextsCfg = config.getConfig("context")
    val contexts = contextsCfg.root().entrySet().filter(entry => {
      entry.getValue.valueType() == ConfigValueType.OBJECT
    }).map(entry => {
      val name = entry.getKey
      val cfg = contextsCfg.getConfig(name).withFallback(defaultCfg)
      name -> ContextConfig(name, cfg)
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

  val disabled = SecurityConfig(enabled = false, "", "", 1.second)
}

case class MasterConfig(
  cluster: EndpointConfig,
  http: HttpConfig,
  mqtt: AsyncInterfaceConfig,
  kafka: AsyncInterfaceConfig,
  logs: LogServiceConfig,
  workers: WorkersSettingsConfig,
  contextsSettings: ContextsSettings,
  dbPath: String,
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
      http = HttpConfig(mist.getConfig("http")),
      mqtt = AsyncInterfaceConfig(mist.getConfig("mqtt")),
      kafka = AsyncInterfaceConfig(mist.getConfig("kafka")),
      logs = LogServiceConfig(mist.getConfig("log-service")),
      workers = WorkersSettingsConfig(mist.getConfig("workers")),
      contextsSettings = ContextsSettings(mist),
      dbPath = mist.getString("db.filepath"),
      security = SecurityConfig(mist.getConfig("security")),
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
