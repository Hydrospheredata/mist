package io.hydrosphere.mist

import java.util.concurrent.TimeUnit

import com.typesafe.config.{ConfigValue, ConfigException, ConfigFactory, Config}

import scala.collection.JavaConversions._
import scala.concurrent.duration.{FiniteDuration, Duration}

/** Configuration wrapper */
private[mist] object MistConfig {

  private val config = ConfigFactory.load()

  val akkaConfig = config.getConfig("mist").withOnlyPath("akka")

  object Akka {

    trait AkkaSettings {
      def settings: Config = config.getConfig("mist").withOnlyPath("akka")

      lazy val serverList = settings.getStringList("akka.cluster.seed-nodes").toList
      lazy val port = settings.getInt("akka.remote.netty.tcp.port")
    }

    object Worker extends AkkaSettings {
      override def settings: Config = super.settings.withFallback(config.getConfig("mist.worker"))
    }

    object Main extends AkkaSettings {
      override def settings: Config = super.settings.withFallback(config.getConfig("mist.main"))
    }
  }

  /** Common application settings */
  object Settings {
    private val settings = config.getConfig("mist.settings")

    /** Max number of threads for JVM where jobs are running */
    lazy val threadNumber: Int = settings.getInt("thread-number")
  }

  /** HTTP specific settings */
  object HTTP {
    private val http = config.getConfig("mist.http")

    /** To start HTTP server or not to start */
    val isOn: Boolean = http.getBoolean("on")

    /** HTTP server host */
    lazy val host: String = http.getString("host")
    /** HTTP server port */
    lazy val port: Int = http.getInt("port")
  }

  /** Settings for each spark context */
  object Spark {
    private val spark = config.getConfig("mist.spark")

    /** Spark master server url
      *
      * Any clear for spark string:
      * local[*]
      * spark://host:7077
      * mesos://host:5050
      * yarn
      */
    lazy val master: String = spark.getString("master")
  }

  /** Hive Test*/

  object Hive {
    lazy val hivetest: Boolean = {
      try {
        config.getBoolean("mist.hive.test")
      } catch {
        case _ => false
      }
    }
  }

  /** MQTT specific settings */
  object MQTT {
    private val mqtt = config.getConfig("mist.mqtt")

    /** To start MQTT subscriber on not to start */
    val isOn: Boolean = mqtt.getBoolean("on")

    /** MQTT host */
    lazy val host: String = mqtt.getString("host")
    /** MQTT port */
    lazy val port: Int = mqtt.getInt("port")
    /** MQTT topic used for ''reading'' */
    lazy val subscribeTopic: String = mqtt.getString("subscribe-topic")
    /** MQTT topic used for ''writing'' */
    lazy val publishTopic: String = mqtt.getString("publish-topic")

  }

  object Recovery {
    private val recovery = config.getConfig("mist.recovery")

    /** job recovery after mist Failure */
    lazy val recoveryOn: Boolean = recovery.getBoolean("on")
    /** job recovery multi start limit */
    lazy val recoveryMultilimit: Int = recovery.getInt("multilimit")
    /** type Db */
    lazy val recoveryTypeDb: String = recovery.getString("typedb")
    /** job recovery MapDb file name */
    lazy val recoveryDbFileName: String = recovery.getString("dbfilename")
  }


  /** Settings for all contexts generally and for each context particularly */
  object Contexts {
    private val contexts = getConfigOption(config, "mist.context")
    private val contextDefaults = config.getConfig("mist.context-defaults")
    private val contextSettings = getConfigOption(config, "mist.context-settings")

    /** Flag of context creating on start or on demand */
    lazy val precreated: List[String] = {
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
    def timeout(contextName: String): FiniteDuration = {
      FiniteDuration(Duration(getContextOrDefault(contextName).getString("timeout")).toNanos, TimeUnit.NANOSECONDS)
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

    /** If true we`ll SparkSession enableHiveSupport */
    def enableHiveSupport(contextName: String): Boolean = {
      getContextOrDefault(contextName).getBoolean("enableHiveSupport")
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
