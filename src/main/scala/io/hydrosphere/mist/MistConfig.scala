package io.hydrosphere.mist

import java.io.File

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import org.apache.spark.streaming.{Duration => SDuration}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

import ConfigUtils._

case class AsyncInterfaceConfig(
  isOn: Boolean,
  host: String,
  port: Int,
  publishTopic: String,
  subscribeTopic: String
)

case class HttpConfig(
  host: String,
  port: Int
)

object HttpConfig {

  def apply(config: Config): HttpConfig =
    HttpConfig(config.getString("host"), config.getInt("port"))

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

  implicit def toScalaDuration(d: java.time.Duration): Duration = Duration.fromNanos(d.toNanos)

  def apply(name: String, config: Config): ContextConfig = {
    ContextConfig(
      name = name,
      sparkConf = config.getConfig("spark-conf").entrySet()
        .filter(entry => entry.getValue.valueType() == ConfigValueType.STRING)
        .map(entry => entry.getKey -> entry.getValue.unwrapped().asInstanceOf[String])
        .toMap,
      downtime = config.getDuration("downtime"),
      maxJobs = config.getInt("max-parallel-jobs"),
      precreated = config.getBoolean("precreated"),
      runOptions = config.getString("run-options"),
      streamingDuration = config.getDuration("streaming-duration")
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
  http: HttpConfig,
  mqtt: AsyncInterfaceConfig,
  kafka: AsyncInterfaceConfig,
  logs: LogServiceConfig,
  workersConfig: WorkersSettingsConfig,
  contextsSettings: ContextsSettings,
  dbPath: String
)

object MasterConfig {

  def load(f: File): MasterConfig = parse(ConfigFactory.parseFile(f).resolve())

  def load(): MasterConfig = parse(ConfigFactory.load())

  def parse(config: Config): MasterConfig = {
    val mist = config.getConfig("mist")
    MasterConfig(
      http = HttpConfig(mist.getConfig("http")),
      mqtt = AsyncInterfaceConfig(mist.getConfig("mqtt")),
      kafka = AsyncInterfaceConfig(mist.getConfig("kafka")),
      logs = LogServiceConfig(mist.getConfig("log-service")),
      workersConfig = WorkersSettingsConfig(mist.getConfig("workers")),
      contextsSettings = ContextsSettings(mist),
      dbPath = mist.getString("db.filepath")
    )
  }

}

case class WorkerConfig(
  logs: LogServiceConfig,
  contextsSettings: ContextsSettings
)

object WorkerConfig {

  def load(f: File): WorkerConfig = {
    val config = ConfigFactory.parseFile(f)
    parse(config)
  }

  def parse(config: Config): WorkerConfig = {
    val mist = config.getConfig("mist")
    WorkerConfig(
      logs = LogServiceConfig(mist.getConfig("log-service")),
      contextsSettings = ContextsSettings(mist)
    )
  }
}

object ConfigUtils {

  import scala.reflect.runtime.universe._

  implicit class OptOpts(config: Config) {

    def getOpt[A: TypeTag](path: String): Option[A] = {
      val value = typeOf[A] match {
        case t if t =:= typeOf[String] => extract[String](path, config.getString)
        case t if t =:= typeOf[Int] => extract[Int](path, config.getInt)
        case t => throw new IllegalArgumentException(s"Not implemented for $t")
      }
      value.asInstanceOf[Option[A]]
    }

    private def extract[A](path: String, f: String => A): Option[A] =
      if (config.hasPath(path)) Option(f(path)) else None
  }
}

/** Configuration wrapper */
//object MistConfig {
//
//  private val config = ConfigFactory.load().resolve()
//
//  def akkaConfig: Config = config.getConfig("mist").withOnlyPath("akka")
//
//  object Akka {
//
//    trait AkkaSettings {
//      def settings: Config = config.getConfig("mist").withOnlyPath("akka")
//
//      def serverList: List[String] = settings.getStringList("akka.cluster.seed-nodes").toList
//      def port: Int = settings.getInt("akka.remote.netty.tcp.port")
//    }
//
//    object Worker extends AkkaSettings {
//      override def settings: Config = super.settings.withFallback(config.getConfig("mist.worker"))
//    }
//
//    object Main extends AkkaSettings {
//      override def settings: Config = super.settings.withFallback(config.getConfig("mist.main"))
//    }
//
//    object CLI extends AkkaSettings {
//      override def settings: Config = super.settings.withFallback(config.getConfig("mist.cli"))
//    }
//  }
//
//  /** Common application settings */
//  object Settings {
//    private def settings: Config = config.getConfig("mist.settings")
//
//    /** Max number of threads for JVM where jobs are running */
//    def threadNumber: Int = settings.getInt("thread-number")
//
//    /** Single JVM mode only for easier run for development */
//    def singleJVMMode: Boolean = settings.getBoolean("single-jvm-mode")
//  }
//
//  /** HTTP specific settings */
//  object Http {
//    private def http: Config = config.getConfig("mist.http")
//
//    /** To start HTTP server or not to start */
//    def isOn: Boolean = http.getBoolean("on")
//
//    /** HTTP server host */
//    def host: String = http.getString("host")
//    /** HTTP server port */
//    def port: Int = http.getInt("port")
//  }
//
//  val Mqtt = AsyncInterfaceConfig(config.getConfig("mist.mqtt"))
//  val Kafka = AsyncInterfaceConfig(config.getConfig("mist.kafka"))
//
//  val LogService = LogServiceConfig(config.getConfig("mist.log_service"))
//
//  object Recovery {
//    private def recovery: Config = config.getConfig("mist.recovery")
//
//    /** job recovery after mist Failure */
//    def recoveryOn: Boolean = recovery.getBoolean("on")
//  }
//
//  object History {
//    private def history = config.getConfig("mist.history")
//
//    def isOn: Boolean = history.getBoolean("on")
//    def filePath: String = history.getString("filepath")
//    def dbType: String = history.getString("type")
//  }
//
//  /** Workers specific settings */
//  object Workers {
//    private def workers: Config = config.getConfig("mist.workers")
//
//    /** Run workers (local, docker, manual) */
//    def runner: String = workers.getString("runner")
//
//    /** Runner server host */
//    def dockerHost: String = workers.getString("docker-host")
//
//    /** Runner server port */
//    def dockerPort: Int = workers.getInt("docker-port")
//
//    /** Shell command for manual running */
//    def cmd: String = workers.getString("cmd")
//
//    /** Shell command for manual stop hooks */
//    def cmdStop: String = workers.getString("cmdStop")
//  }
//
//  /** Settings for all contexts generally and for each context particularly */
//  object Contexts {
//    private def contexts: Option[Config] = getConfigOption(config, "mist.context")
//    private def contextDefaults: Config = config.getConfig("mist.context-defaults")
//    private def contextSettings: Option[Config] = getConfigOption(config, "mist.context-settings")
//
//    /** Flag of context creating on start or on demand */
//    def precreated: List[String] = {
//      contextSettings match {
//        case Some(cnf) => cnf.getStringList("onstart").toList
//        case None => List()
//      }
//    }
//
//    /** Return config for specified context or default settings
//      *
//      * @param contextName    context name
//      * @return               config for `contextName` or default config
//      */
//    private def getContextOrDefault(contextName: String): Config = {
//      try {
//        contexts match {
//          case Some(cnf) => cnf.getConfig(contextName).withFallback(contextDefaults)
//          case None => contextDefaults
//        }
//      }
//      catch {
//        case _: ConfigException.Missing =>
//          contextDefaults
//      }
//    }
//
//    /** Waiting for job completion timeout */
//    def timeout(contextName: String): Duration = {
//      Duration(getContextOrDefault(contextName).getString("timeout"))
//    }
//
//    /** If true we'll stop context */
//    def isDisposable(contextName: String): Boolean = {
//      getContextOrDefault(contextName).getBoolean("disposable")
//    }
//
//    /** Settings for SparkConf */
//    def sparkConf(contextName: String): Set[List[String]] = {
//      getContextOrDefault(contextName).getConfig("spark-conf").entrySet.map {
//        case (m: java.util.Map.Entry[String, ConfigValue]) => List(m.getKey, m.getValue.unwrapped().toString)
//      }.toSet
//    }
//
//    def runOptions(namespace: String): String = {
//      getContextOrDefault(namespace).getString("run-options")
//    }
//
//    def streamingDuration(contextName: String): SDuration = {
//      SDuration(Duration(getContextOrDefault(contextName).getString("streaming-duration")).toMillis)
//    }
//
//    def maxParallelJobs(contextName: String): Int = {
//      getContextOrDefault(contextName).getInt("max-parallel-jobs")
//    }
//
//    /** Downtime worker's before stopping*/
//    def downtime(contextName: String): Duration = {
//      Duration(getContextOrDefault(contextName).getString("worker-downtime"))
//    }
//
//  }
//
//  private def getConfigOption(config: Config, path: String): Option[Config] = {
//    if (config.hasPath(path)) {
//      Some(config.getConfig(path))
//    } else {
//      None
//    }
//  }
//}
