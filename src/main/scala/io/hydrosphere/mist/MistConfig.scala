package io.hydrosphere.mist

import com.typesafe.config.{Config, ConfigException, ConfigFactory, ConfigValue}
import io.hydrosphere.mist.master.interfaces.async.AsyncInterface
import org.apache.spark.streaming.{Duration => SDuration}

import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration

case class AsyncInterfaceConfig(
  isOn: Boolean,
  host: String,
  port: Int,
  publishTopic: String,
  subscribeTopic: String
)

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

/** Configuration wrapper */
object MistConfig {

  private val config = ConfigFactory.load().resolve()

  def akkaConfig: Config = config.getConfig("mist").withOnlyPath("akka")

  object Akka {

    trait AkkaSettings {
      def settings: Config = config.getConfig("mist").withOnlyPath("akka")

      def serverList: List[String] = settings.getStringList("akka.cluster.seed-nodes").toList
      def port: Int = settings.getInt("akka.remote.netty.tcp.port")
    }

    object Worker extends AkkaSettings {
      override def settings: Config = super.settings.withFallback(config.getConfig("mist.worker"))
    }

    object Main extends AkkaSettings {
      override def settings: Config = super.settings.withFallback(config.getConfig("mist.main"))
    }

    object CLI extends AkkaSettings {
      override def settings: Config = super.settings.withFallback(config.getConfig("mist.cli"))
    }
  }

  /** Common application settings */
  object Settings {
    private def settings: Config = config.getConfig("mist.settings")

    /** Max number of threads for JVM where jobs are running */
    def threadNumber: Int = settings.getInt("thread-number")

    /** Single JVM mode only for easier run for development */
    def singleJVMMode: Boolean = settings.getBoolean("single-jvm-mode")
  }

  /** HTTP specific settings */
  object Http {
    private def http: Config = config.getConfig("mist.http")

    /** To start HTTP server or not to start */
    def isOn: Boolean = http.getBoolean("on")

    /** HTTP server host */
    def host: String = http.getString("host")
    /** HTTP server port */
    def port: Int = http.getInt("port")
  }

  /** Hive Test*/

  object Hive {
    def hiveTest: Boolean = {
      try {
        config.getBoolean("mist.hive.test")
      } catch {
        case _: Throwable => false
      }
    }
  }
  
  val Mqtt = AsyncInterfaceConfig(config.getConfig("mist.mqtt"))
  val Kafka = AsyncInterfaceConfig(config.getConfig("mist.kafka"))

  object Recovery {
    private def recovery: Config = config.getConfig("mist.recovery")

    /** job recovery after mist Failure */
    def recoveryOn: Boolean = recovery.getBoolean("on")
  }
  
  object History {
    private def history = config.getConfig("mist.history")
    
    def isOn: Boolean = history.getBoolean("on")
    def filePath: String = history.getString("filepath")
    def dbType: String = history.getString("type")
  }

  /** Workers specific settings */
  object Workers {
    private def workers: Config = config.getConfig("mist.workers")

    /** Run workers (local, docker, manual) */
    def runner: String = workers.getString("runner")

    /** Runner server host */
    def dockerHost: String = workers.getString("docker-host")

    /** Runner server port */
    def dockerPort: Int = workers.getInt("docker-port")

    /** Shell command for manual running */
    def cmd: String = workers.getString("cmd")

    /** Shell command for manual stop hooks */
    def cmdStop: String = workers.getString("cmdStop")
  }

  /** Settings for all contexts generally and for each context particularly */
  object Contexts {
    private def contexts: Option[Config] = getConfigOption(config, "mist.context")
    private def contextDefaults: Config = config.getConfig("mist.context-defaults")
    private def contextSettings: Option[Config] = getConfigOption(config, "mist.context-settings")

    /** Flag of context creating on start or on demand */
    def precreated: List[String] = {
      contextSettings match {
        case Some(cnf) => cnf.getStringList("onstart").toList
        case None => List()
      }
    }

    /** Return config for specified context or default settings
      *
      * @param contextName    context name
      * @return               config for `contextName` or default config
      */
    private def getContextOrDefault(contextName: String): Config = {
      try {
        contexts match {
          case Some(cnf) => cnf.getConfig(contextName).withFallback(contextDefaults)
          case None => contextDefaults
        }
      }
      catch {
        case _: ConfigException.Missing =>
          contextDefaults
      }
    }

    /** Waiting for job completion timeout */
    def timeout(contextName: String): Duration = {
      Duration(getContextOrDefault(contextName).getString("timeout"))
    }

    /** If true we'll stop context */
    def isDisposable(contextName: String): Boolean = {
      getContextOrDefault(contextName).getBoolean("disposable")
    }

    /** Settings for SparkConf */
    def sparkConf(contextName: String): Set[List[String]] = {
      getContextOrDefault(contextName).getConfig("spark-conf").entrySet.map {
        case (m: java.util.Map.Entry[String, ConfigValue]) => List(m.getKey, m.getValue.unwrapped().toString)
      }.toSet
    }

    def runOptions(namespace: String): String = {
      getContextOrDefault(namespace).getString("run-options")
    }

    def streamingDuration(contextName: String): SDuration = {
      SDuration(Duration(getContextOrDefault(contextName).getString("streaming-duration")).toMillis)
    }
    
    def maxParallelJobs(contextName: String): Int = {
      getContextOrDefault(contextName).getInt("max-parallel-jobs")
    }

    /** Downtime worker's before stopping*/
    def downtime(contextName: String): Duration = {
      Duration(getContextOrDefault(contextName).getString("worker-downtime"))
    }

  }

  private def getConfigOption(config: Config, path: String): Option[Config] = {
    if (config.hasPath(path)) {
      Some(config.getConfig(path))
    } else {
      None
    }
  }
}
