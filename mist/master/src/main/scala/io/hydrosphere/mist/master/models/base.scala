package io.hydrosphere.mist.master.models

import scala.concurrent.duration.Duration

/** Specify how use context/workers */
sealed trait RunMode {

  def name: String = this match {
    case RunMode.Shared => "shared"
    case RunMode.ExclusiveContext => "exclusive"
  }

}

object RunMode {

  def fromName(n: String): RunMode = n match {
    case "shared" => RunMode.Shared
    case "exclusive" => RunMode.ExclusiveContext
    case x => throw new IllegalArgumentException(s"Unknown mode $x")
  }
  /** Job will share one worker with jobs that are running on the same namespace */
  case object Shared extends RunMode
  /** There will be created unique worker for job execution */
  case object ExclusiveContext extends RunMode

}

trait NamedConfig {
  val name: String
}

sealed trait LaunchData
/** use default worker-runner **/
case object ServerDefault extends LaunchData

object EMRInstance {

  sealed trait InstanceGroupType
  object InstanceGroupType {
    case object Master extends InstanceGroupType
    case object Core extends InstanceGroupType
    case object Task extends InstanceGroupType
  }

  sealed trait Market
  object Market {
    case object OnDemand extends Market
    case object Spot extends Market
  }

  sealed trait VolumeType
  object VolumeType {
    case object Standard extends VolumeType
    case object IO1 extends VolumeType
    case object GP2 extends VolumeType
  }
  final case class EbsVolume(
    volumeType: VolumeType,
    sizeGB: Int,
    iops: Int,
    count: Option[Int]
  )

  final case class Ebs(
    optimized: Option[Boolean],
    volumes: Seq[EbsVolume]
  )

  sealed trait AdjustmentType
  object AdjustmentType {
    case object ChangeInCapacity extends AdjustmentType
    case object PercentChangeInCapacity extends AdjustmentType
    case object ExactCapacity extends AdjustmentType
  }

  sealed trait ComparisonOperator
  object ComparisonOperator {
    case object GreaterThanOrEqual extends ComparisonOperator
    case object GreaterThan extends ComparisonOperator
    case object LessThan extends ComparisonOperator
    case object LessThanOrEqual extends ComparisonOperator
  }
  sealed trait Statistic
  object Statistic {
    case object SampleCount extends Statistic
    case object Average extends Statistic
    case object Sum extends Statistic
    case object Minimum extends Statistic
    case object Maximum extends Statistic
  }

  final case class Dimension(key:String, value: String)

  final case class Trigger(
    comparisonOperator: ComparisonOperator,
    evaluationPeriods: Int,
    metricName: String,
    namespace: String,
    period: Int,
    threshold: Double,
    statistic: Statistic,
    unit: String,
    dimensions: Seq[Dimension]
  )

  final case class Rule(
    name: String,
    description: String,
    adjustmentType: AdjustmentType,
    scalingAdjustment: Int,
    coolDown: Int,
    trigger: Trigger
  )

  final case class AutoScaling(
    max: Int,
    min: Int,
    rules: Seq[Rule]
  )

  final case class Instance(
    instanceType: String,
    instanceGroupType: InstanceGroupType,
    name: Option[String],
    instanceCount: Int,
    market: Option[Market],
    ebs: Option[Ebs],
    bidPrice: Option[String],
    autoScaling: Option[AutoScaling]
  )

}

final case class AWSEMRLaunchData(
  launcherSettingsName: String,
  releaseLabel: String,
  instances: Seq[EMRInstance.Instance]
) extends LaunchData

final case class ContextConfig(
  name: String,
  sparkConf: Map[String, String],
  downtime: Duration,
  maxJobs: Int,
  precreated: Boolean,
  runOptions: String,
  workerMode: RunMode,
  streamingDuration: Duration,
  maxConnFailures: Int,
  launchData: LaunchData
) extends NamedConfig


final case class FunctionConfig(
  name: String,
  path: String,
  className: String,
  defaultContext: String
) extends NamedConfig
