package io.hydrosphere.mist.master.data

import com.typesafe.config.{Config, ConfigValue, ConfigValueFactory, ConfigValueType}
import io.hydrosphere.mist.master.models._
import io.hydrosphere.mist.utils.ConfigUtils._

import scala.concurrent.duration._


trait LowConfigRepr[A] {

  def toConfig(a: A): Config

  def fromConfig(config: Config): A
}

trait ConfigRepr[A <: NamedConfig] extends LowConfigRepr[A] {

  def toConfig(a: A): Config

  def fromConfig(config: Config): A

  def fromConfig(name: String, config: Config): A =
    fromConfig(config.withValue("name", ConfigValueFactory.fromAnyRef(name)))
}

object ConfigRepr {

  import scala.collection.JavaConverters._

  implicit class ToConfigSyntax[A <: NamedConfig](a: A)(implicit repr: ConfigRepr[A]) {
    def toConfig: Config = repr.toConfig(a)
  }

  implicit class FromCongixSyntax(config: Config){
    def to[A <: NamedConfig](implicit repr: ConfigRepr[A]): A = repr.fromConfig(config)
    def to[A <: NamedConfig](name: String)(implicit repr: ConfigRepr[A]): A = repr.fromConfig(name, config)
  }

  implicit val EndpointsRepr = new ConfigRepr[FunctionConfig] {

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

  val ClusterConfigRepr: LowConfigRepr[ClusterConfig] = new LowConfigRepr[ClusterConfig] {
    override def fromConfig(config: Config): ClusterConfig = {
      config.getString("type") match {
        case "no-cluster" => NoCluster
        case "aws-emr" => AwsEMRConfig(
          provisionerId = config.getString("provisioner-id"),
          releaseLabel = config.getString("release-label"),
          masterInstanceType = config.getString("master-instance-type"),
          slaveInstanceType = config.getString("slave-instance-type"),
          instanceCount = config.getInt("instance-count")
        )
        case x => throw new IllegalArgumentException(s"Unknown cluster type $x")
      }
    }

    override def toConfig(cfg: ClusterConfig): Config = {
      import ConfigValueFactory._

      val out = cfg match {
        case NoCluster => "no-cluster" -> Map.empty[String, ConfigValue]
        case awsEmr: AwsEMRConfig =>
          "aws-emr" -> Map(
            "provisioner-id" -> fromAnyRef(awsEmr.provisionerId),
            "release-label" -> fromAnyRef(awsEmr.releaseLabel),
            "master-instance-type" -> fromAnyRef(awsEmr.masterInstanceType),
            "slave-instance-type" -> fromAnyRef(awsEmr.slaveInstanceType),
            "instance-count" -> fromAnyRef(awsEmr.instanceCount)
          )
      }
      val appendType = out._2 + ("type" -> fromAnyRef(out._1))
      fromMap(appendType.asJava).toConfig
    }
  }

  implicit val ContextConfigRepr: ConfigRepr[ContextConfig] = new ConfigRepr[ContextConfig] {

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
        clusterConfig =
          config.getOptConfig("cluster-config")
            .map(ClusterConfigRepr.fromConfig)
            .getOrElse(NoCluster)
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
        "cluster-config" -> ClusterConfigRepr.toConfig(a.clusterConfig).root()
      )
      fromMap(map.asJava).toConfig
    }
  }
}

