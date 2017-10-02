package io.hydrosphere.mist.master.data

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import io.hydrosphere.mist.master.models.{ContextConfig, EndpointConfig, NamedConfig}

import scala.concurrent.duration._

trait ConfigRepr[A <: NamedConfig] {

  def toConfig(a: A): Config

  def fromConfig(config: Config): A

  def fromConfig(name: String, config: Config): A =
    fromConfig(config.withValue("name", ConfigValueFactory.fromAnyRef(name)))
}

object ConfigRepr {

  import scala.collection.JavaConverters._

  implicit val EndpointsRepr = new ConfigRepr[EndpointConfig] {

    override def toConfig(a: EndpointConfig): Config = {
      import ConfigValueFactory._
      val map = Map(
        "path" -> fromAnyRef(a.path),
        "className" -> fromAnyRef(a.className),
        "namespace" -> fromAnyRef(a.defaultContext)
      )
      fromMap(map.asJava).toConfig
    }

    override def fromConfig(config: Config): EndpointConfig = {
      EndpointConfig(
        config.getString("name"),
        config.getString("path"),
        config.getString("className"),
        config.getString("namespace")
      )
    }
  }

  implicit val ContextConfigRepr = new ConfigRepr[ContextConfig] {

    val allowedTypes = Set(
      ConfigValueType.STRING,
      ConfigValueType.NUMBER,
      ConfigValueType.BOOLEAN
    )

    override def fromConfig(config: Config): ContextConfig = {
      ContextConfig(
        name = config.getString("name"),
        sparkConf = config.getConfig("spark-conf").entrySet().asScala
          .filter(entry => allowedTypes.contains(entry.getValue.valueType()))
          .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
          .toMap,
        downtime = Duration(config.getString("downtime")),
        maxJobs = config.getInt("max-parallel-jobs"),
        precreated = config.getBoolean("precreated"),
        workerMode = config.getString("worker-mode"),
        runOptions = config.getString("run-options"),
        streamingDuration = Duration(config.getString("streaming-duration"))
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
        "worker-mode" -> fromAnyRef(a.workerMode),
        "run-options" -> fromAnyRef(a.runOptions),
        "streaming-duration" -> fromDuration(a.streamingDuration)
      )
      fromMap(map.asJava).toConfig
    }
  }
}

