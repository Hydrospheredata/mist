package io.hydrosphere.mist.master.data

import com.typesafe.config._
import io.hydrosphere.mist.master.interfaces.{EMRInstanceFormat, JsonCodecs}
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.ConfigUtils._
import spray.json.{JsonFormat, JsonParser, ParserInput}

import scala.concurrent.duration._

trait ConfigRepr[A] {
  def toConfig(a: A): Config
  def fromConfig(config: Config): A
}

trait NamedConfigRepr[A] extends ConfigRepr[A] {

  def fromConfig(name: String, config: Config): A =
    fromConfig(config.withValue("name", ConfigValueFactory.fromAnyRef(name)))

}

//TODO it's the same as jsonCodecs!
object ConfigRepr {

  import scala.collection.JavaConverters._

  implicit class ToConfigSyntax[A <: NamedConfig](a: A)(implicit repr: ConfigRepr[A]) {
    def toConfig: Config = repr.toConfig(a)
  }

  val EndpointsRepr = new NamedConfigRepr[FunctionConfig] {

    override def toConfig(a: FunctionConfig): Config = {
      import ConfigValueFactory._
      val map = Map(
        "path" -> fromAnyRef(a.path),
        "className" -> fromAnyRef(a.className),
        "namespace" -> fromAnyRef(a.defaultContext)
      )
      fromMap(map.asJava).toConfig
    }

    override def fromConfig(config: Config): FunctionConfig = {
      FunctionConfig(
        config.getString("name"),
        config.getString("path"),
        config.getString("className"),
        config.getString("namespace")
      )
    }
  }

  def fromJsonFormat[A](jsonFormat: JsonFormat[A]): ConfigRepr[A] = new ConfigRepr[A] {
    override def toConfig(a: A): Config = {
      val s = jsonFormat.write(a).prettyPrint
      ConfigFactory.parseString(s)
    }
    override def fromConfig(config: Config): A = {
      val jsonString = config.root().render(ConfigRenderOptions.defaults().setJson(true).setComments(false).setOriginComments(false))
      val json = JsonParser(ParserInput(jsonString))
      jsonFormat.read(json)
    }
  }

  val EMRInstancesRepr: ConfigRepr[EMRInstances] = fromJsonFormat(EMRInstanceFormat)

  val LaunchDataConfigRepr: ConfigRepr[LaunchData] = new ConfigRepr[LaunchData] {

    override def toConfig(a: LaunchData): Config = {
      import ConfigValueFactory._

      val (t, body) = a match {
        case ServerDefault => "server-default" -> Map.empty[String, ConfigValue]
        case awsEmr: AWSEMRLaunchData =>
          val instances = EMRInstancesRepr.toConfig(awsEmr.instances)
          "aws-emr" -> Map(
            "launcher-settings-name" -> fromAnyRef(awsEmr.launcherSettingsName),
            "release-label" -> fromAnyRef(awsEmr.releaseLabel),
            "instances" -> instances.root()
          )
      }
      val full = body + ("type" -> t)
      fromMap(full.asJava).toConfig
    }

    override def fromConfig(config: Config): LaunchData = {
      config.getString("type") match {
        case "server-default" => ServerDefault
        case "aws-emr" =>
          AWSEMRLaunchData(
            launcherSettingsName = config.getString("launcher-settings-name"),
            releaseLabel = config.getString("release-label"),
            instances = EMRInstancesRepr.fromConfig(config.getConfig("instances"))
          )
        case x => throw new IllegalArgumentException(s"Unknown launch data type $x")
      }
    }
  }

  val ContextConfigRepr: NamedConfigRepr[ContextConfig] = new NamedConfigRepr[ContextConfig] {

    val allowedTypes = Set(
      ConfigValueType.STRING,
      ConfigValueType.NUMBER,
      ConfigValueType.BOOLEAN
    )

    override def fromConfig(config: Config): ContextConfig = {
      def runMode(s: String): RunMode = s match {
        case "shared" => RunMode.Shared
        case "exclusive" => RunMode.ExclusiveContext
      }

      def cleanKey(key: String): String = key.replaceAll("\"", "")

      ContextConfig(
        name = config.getString("name"),
        sparkConf = config.getConfig("spark-conf").entrySet().asScala
          .filter(entry => allowedTypes.contains(entry.getValue.valueType()))
          .map(entry => cleanKey(entry.getKey) -> entry.getValue.unwrapped().toString)
          .toMap,
        downtime = Duration(config.getString("downtime")),
        maxJobs = config.getOptInt("max-jobs").getOrElse(config.getInt("max-parallel-jobs")),
        precreated = config.getBoolean("precreated"),
        workerMode = runMode(config.getString("worker-mode")) ,
        runOptions = config.getString("run-options"),
        streamingDuration = Duration(config.getString("streaming-duration")),
        maxConnFailures = config.getOptInt("max-conn-failures").getOrElse(5),
        launchData = config.getOptConfig("launch-data").map(LaunchDataConfigRepr.fromConfig).getOrElse(ServerDefault)
      )
    }

    override def toConfig(a: ContextConfig): Config = {
      import ConfigValueFactory._

      def fromDuration(d: Duration): ConfigValue = {
        d match {
          case f: FiniteDuration => fromAnyRef(s"${f.toSeconds}s")
          case inf => fromAnyRef("Inf")
        }
      }
      val map = Map(
        "spark-conf" -> fromMap(a.sparkConf.asJava),
        "downtime" -> fromDuration(a.downtime),
        "max-parallel-jobs" -> fromAnyRef(a.maxJobs),
        "precreated" -> fromAnyRef(a.precreated),
        "worker-mode" -> fromAnyRef(a.workerMode.name),
        "run-options" -> fromAnyRef(a.runOptions),
        "streaming-duration" -> fromDuration(a.streamingDuration),
        "max-conn-failures" -> fromAnyRef(a.maxConnFailures),
        "launch-data" -> LaunchDataConfigRepr.toConfig(a.launchData).root()
      )
      fromMap(map.asJava).toConfig
    }
  }
}

