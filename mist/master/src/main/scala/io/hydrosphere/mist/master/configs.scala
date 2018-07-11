package io.hydrosphere.mist.master

import java.io.File

import cats.Eval
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory, ConfigValueType}
import io.hydrosphere.mist.utils.ConfigUtils._
import io.hydrosphere.mist.master.data.ConfigRepr
import io.hydrosphere.mist.master.models.ContextConfig
import cats._
import cats.syntax._
import cats.implicits._
import io.hydrosphere.mist.utils.{Logger, NetUtils}

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
  uiPath: String,
  keepAliveTick: FiniteDuration
)

object HttpConfig {

  def apply(config: Config): HttpConfig =
    HttpConfig(
      config.getString("host"),
      config.getInt("port"),
      config.getString("ui"),
      config.getFiniteDuration("ws-keepalive-tick")
    )
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
  readyTimeout: FiniteDuration,
  maxArtifactSize: Long,
  dockerConfig: DockerRunnerConfig,
  manualConfig: ManualRunnerConfig
)

sealed trait DockerNetworkConfiguration
case class NamedNetwork(name: String) extends DockerNetworkConfiguration
case class AutoMasterNetwork(masterId: String) extends DockerNetworkConfiguration

object DockerNetworkConfiguration {

  def apply(config: Config): DockerNetworkConfiguration = {
    config.getString("network-type") match {
      case "auto-master" => AutoMasterNetwork(config.getConfig("auto-master-network").getString("container-id"))
      case name => NamedNetwork(name)
    }
  }
}

case class DockerRunnerConfig(
  dockerHost: String,
  image: String,
  network: DockerNetworkConfiguration,
  mistHome: String,
  sparkHome: String
)

object DockerRunnerConfig {
  def apply(config: Config): DockerRunnerConfig = {
    DockerRunnerConfig(
      dockerHost = config.getString("host"),
      image = config.getString("image"),
      network = DockerNetworkConfiguration(config),
      mistHome = config.getString("mist-home"),
      sparkHome = config.getString("spark-home")
    )
  }
}

case class ManualRunnerConfig(
  cmdStart: String,
  cmdStop: Option[String],
  async: Boolean
)

object ManualRunnerConfig {
  def apply(config: Config): ManualRunnerConfig = {
    def readOld(): ManualRunnerConfig = {
      val stop = config.getString("cmdStop")
      ManualRunnerConfig(
        cmdStart = config.getString("cmd"),
        cmdStop = if (stop.isEmpty) None else stop.some,
        async = true
      )
    }

    def readNew(): ManualRunnerConfig = {
      val entry = config.getConfig("manual")
      ManualRunnerConfig(
        cmdStart = entry.getString("startCmd"),
        cmdStop = entry.getOptString("stopCmd"),
        async = entry.getBoolean("async")
      )
    }

    if (config.getString("cmd").nonEmpty) readOld() else readNew()
  }
}

object WorkersSettingsConfig {

  def apply(config: Config): WorkersSettingsConfig = {
    WorkersSettingsConfig(
      runner = config.getString("runner"),
      runnerInitTimeout = Duration(config.getString("runner-init-timeout")),
      readyTimeout = Duration(config.getString("ready-timeout")) match {
        case f: FiniteDuration => f
        case _ => throw new IllegalArgumentException("Worker ready-teimout should be finite")
      },
      maxArtifactSize = config.getBytes("max-artifact-size"),
      dockerConfig = DockerRunnerConfig(config.getConfig("docker")),
      manualConfig = ManualRunnerConfig(config)
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

case class FunctionInfoProviderConfig(
  runTimeout: FiniteDuration,
  cacheEntryTtl: FiniteDuration,
  sparkConf: Map[String, String]
)
object FunctionInfoProviderConfig {
  import scala.collection.JavaConverters._

  def apply(c: Config): FunctionInfoProviderConfig = {
    FunctionInfoProviderConfig(
      c.getFiniteDuration("init-timeout"),
      c.getFiniteDuration("cache-entry-ttl"),
      c.getConfig("spark-conf").entrySet().asScala
        .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
        .toMap
    )
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

sealed trait ClusterProvisionerConfig
final case class EMRProvisionerConfig(
  keyPair: String,
  localKeyPath: String,
  accessKey: String,
  secretKey: String,
  subnetId: String,
  region: String,
  initTimeout: Duration
) extends ClusterProvisionerConfig

object EMRProvisionerConfig {

  def apply(c: Config): EMRProvisionerConfig = {
    EMRProvisionerConfig(
      keyPair = c.getString("key-pair"),
      accessKey = c.getString("access-key"),
      localKeyPath = c.getString("local-key-path"),
      secretKey = c.getString("secret-key"),
      subnetId = c.getString("subnet-id"),
      region = c.getString("region"),
      initTimeout = c.getOptDuration("init-timeout").getOrElse(Duration.Inf)
    )
  }
}

object ClusterProvisionerConfig {

  def apply(c: Config): Map[String, ClusterProvisionerConfig] = {
    c.root().entrySet().filter(entry => {
      entry.getValue.valueType() == ConfigValueType.OBJECT
    }).map(entry => {
      val name = entry.getKey
      val cfg = c.getConfig(name)
      val parsed = cfg.getString("type") match {
          //TODO - > aws-emr
        case "emr" => EMRProvisionerConfig.apply(cfg)
        case x => throw new IllegalArgumentException(s"Unknown provisioner type: $x")
      }
      name -> parsed
    }).toMap
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
  functionsPath: String,
  security: Option[SecurityConfig],
  jobInfoProviderConfig: FunctionInfoProviderConfig,
  srcConfigPath: String,
  jobsSavePath: String,
  artifactRepositoryPath: String,
  clusterProvisioners: Map[String, ClusterProvisionerConfig],
  raw: Config
)

object MasterConfig extends Logger {

  def load(filePath: String): MasterConfig = {
    val cfg = loadConfig(filePath)
    autoConfigure(parse(filePath, cfg))
  }

  def loadConfig(filePath: String): Config = {
    val user = ConfigFactory.parseFile(new File(filePath))
    resolveUserConf(user)
  }

  def resolveUserConf(config: Config): Config = {
    val appConfig = ConfigFactory.parseResourcesAnySyntax("master.conf")
    val properties = ConfigFactory.systemProperties()
    properties.withFallback(config.withFallback(appConfig)).resolve()
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
      functionsPath = mist.getString("functions-store.path"),
      jobsSavePath = mist.getString("jobs-resolver.save-path"),
      artifactRepositoryPath = mist.getString("artifact-repository.save-path"),
      security = SecurityConfig.ifEnabled(mist.getConfig("security")),
      jobInfoProviderConfig = FunctionInfoProviderConfig(mist.getConfig("job-extractor")),
      srcConfigPath = filePath,
      clusterProvisioners = ClusterProvisionerConfig.apply(mist.getConfig("cluster-provisioners")),
      raw = config
    )
  }

  def autoConfigure(masterConfig: MasterConfig): MasterConfig =
    autoConfigure(masterConfig, Eval.later(NetUtils.findLocalInetAddress().getHostAddress))

  def autoConfigure(masterConfig: MasterConfig, host: Eval[String]): MasterConfig = {
    def isAuto(s: String): Boolean = s == "auto"

    val forCluster = (cfg: MasterConfig) => {
      import cfg._
      if (isAuto(cluster.host)) {
        logger.info(s"Automatically update cluster host to ${host.value}")
        val upd = cluster.copy(host = host.value)
        cfg.copy(cluster = upd).copy(raw = raw.withValue("akka.remote.netty.tcp.hostname", ConfigValueFactory.fromAnyRef(host.value)))
      } else cfg
    }

    val forHttp = (cfg: MasterConfig) => {
      import cfg._

      if (isAuto(http.host)) {
        logger.info(s"Automatically update http host to ${host.value}")
        val upd = http.copy(host = host.value)
        cfg.copy(http = upd)
      } else cfg
    }

    val forLogs = (cfg: MasterConfig) => {
      import cfg._

      if(isAuto(logs.host)) {
        logger.info(s"Automatically update logs host to ${host.value}")
        val upd = logs.copy(host = host.value)
        cfg.copy(logs = upd)
      } else cfg
    }

    val upd = forCluster >>> forHttp >>> forLogs
    upd(masterConfig)
  }

}

