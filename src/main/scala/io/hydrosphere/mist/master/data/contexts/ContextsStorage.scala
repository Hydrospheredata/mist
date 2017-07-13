package io.hydrosphere.mist.master.data.contexts

import java.nio.file.Path

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import io.hydrosphere.mist.master.ContextsSettings
import io.hydrosphere.mist.master.data.{ConfigRepr, FsStorage$}

import scala.concurrent.duration.{Duration, FiniteDuration}


object ContextsStorage {

  implicit val contextConfigRepr = new ConfigRepr[ContextConfig] {

    import scala.collection.JavaConverters._

    val allowedTypes = Set(
      ConfigValueType.STRING,
      ConfigValueType.NUMBER,
      ConfigValueType.BOOLEAN
    )

    override def name(a: ContextConfig): String = a.name

    override def fromConfig(name: String, config: Config): ContextConfig = {
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
        "run-options" -> fromAnyRef(a.runOptions),
        "streaming-duration" -> fromDuration(a.streamingDuration)
      )
      fromMap(map.asJava).toConfig
    }
  }

  def create(dir: String, contextsSettings: ContextsSettings): FsStorage[ContextConfig] = {
//    FileEntryStorage.create[ContextConfig](dir)
//      .withDefaults(contextsSettings)
  }
}
