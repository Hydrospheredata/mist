package io.hydrosphere.mist

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

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
}

object ContextsSettings {

  val Default = "default"

  def apply(config: Config): ContextsSettings = {
    val defaultCfg = config.getConfig("context-defaults")
    val default = ContextConfig("default", defaultCfg)

    val contextsCfg = config.getConfig("context")
    val contexts = contextsCfg.entrySet().filter(entry => {
      entry.getValue.valueType() == ConfigValueType.OBJECT
    }).map(entry => {
      val name = entry.getKey
      val cfg = contextsCfg.getConfig(name).withFallback(defaultCfg)
      name -> ContextConfig(name, cfg)
    }).toMap

    ContextsSettings(default, contexts)
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
