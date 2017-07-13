package io.hydrosphere.mist.master.data.contexts

import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions, ConfigValue, ConfigValueFactory, ConfigValueType}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

case class ContextConfig(
  name: String,
  sparkConf: Map[String, String],
  downtime: Duration,
  maxJobs: Int,
  precreated: Boolean,
  runOptions: String,
  streamingDuration: Duration
) {

  def toRaw: Config = {
    import ConfigValueFactory._

    def fromDuration(d: Duration): ConfigValue = {
      d match {
        case f: FiniteDuration => fromAnyRef(s"${f.toSeconds}s")
        case inf => fromAnyRef("Inf")
      }
    }
    val map = Map(
      "spark-conf" -> fromMap(sparkConf.asJava),
      "downtime" -> fromDuration(downtime),
      "max-parallel-jobs" -> fromAnyRef(maxJobs),
      "precreated" -> fromAnyRef(precreated),
      "run-options" -> fromAnyRef(runOptions),
      "streaming-duration" -> fromDuration(streamingDuration)
    )
    fromMap(map.asJava).toConfig
  }
}

object ContextConfig {

  val allowedTypes = Set(
    ConfigValueType.STRING,
    ConfigValueType.NUMBER,
    ConfigValueType.BOOLEAN
  )

  def fromConfig(name: String, config: Config): ContextConfig = {
    ContextConfig(
      name = name,
      sparkConf = config.getConfig("spark-conf").entrySet().asScala
        .filter(entry => allowedTypes.contains(entry.getValue.valueType()))
        .map(entry => entry.getKey -> entry.getValue.unwrapped().toString)
        .toMap,
      downtime = Duration(config.getString("downtime")),
      maxJobs = config.getInt("max-parallel-jobs"),
      precreated = config.getBoolean("precreated"),
      runOptions = config.getString("run-options"),
      streamingDuration = Duration(config.getString("streaming-duration"))
    )
  }
}

