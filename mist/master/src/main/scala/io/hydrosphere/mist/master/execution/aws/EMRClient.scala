package io.hydrosphere.mist.master.execution.aws

import cats._
import cats.implicits._
import cats.effect._
import io.hydrosphere.mist.master.execution.aws.JFutureSyntax._
import io.hydrosphere.mist.master.models.EMRInstance
import io.hydrosphere.mist.master.models.EMRInstance.{AutoScaling, Ebs, VolumeType}
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.{Unit => _, _}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

trait EMRClient[F[_]] {

  def start(settings: EMRRunSettings, instances: Seq[EMRInstance.Instance]): F[EmrInfo]
  def status(id: String): F[Option[EmrInfo]]
  def stop(id: String): F[Unit]

  def awaitStatus(
    id: String,
    target: EMRStatus,
    sleepTime: FiniteDuration,
    triesLeft: Int
  )(implicit M: MonadError[F, Throwable], T: Timer[F]): F[EmrInfo] = {

    for {
      maybeInfo <- status(id)
      next <- maybeInfo match {
        case Some(info) if info.status == target =>
          M.pure(info)
        case Some(info) if triesLeft > 0 =>
          T.sleep(sleepTime).flatMap(_ => awaitStatus(id, target, sleepTime, triesLeft - 1))
        case None =>
          M.raiseError(new IllegalArgumentException(s"Cluster $id doesn't exists"))
      }
    } yield next
  }
}

object EMRClient {

  class Default(orig: EMRAsyncClient) extends EMRClient[IO] {

    type JFICBuilder = JobFlowInstancesConfig.Builder

    private def ebsConfiguration(ebs: Ebs): EbsConfiguration = {

      val devices = ebs.volumes.map(volume => {
        val volumeType = volume.volumeType match {
          case VolumeType.Standard => "standard"
          case VolumeType.IO1 => "io1"
          case VolumeType.GP2 => "gp2"
        }
        val spec = VolumeSpecification.builder()
          .iops(volume.iops)
          .sizeInGB(volume.sizeGB)
          .volumeType(volumeType)
          .build()

        val count = volume.count.getOrElse(1)
        EbsBlockDeviceConfig.builder()
          .volumeSpecification(spec)
          .volumesPerInstance(count)
          .build()
      })

      val optimized = ebs.optimized.getOrElse(true)
      EbsConfiguration.builder()
        .ebsBlockDeviceConfigs(devices.asJavaCollection)
        .ebsOptimized(optimized)
        .build()
    }

    private def mkAutoScaling(as: AutoScaling): AutoScalingPolicy = {

      def mkRule(rule: EMRInstance.Rule): ScalingRule = {
        val adjType = rule.adjustmentType match {
          case EMRInstance.AdjustmentType.ChangeInCapacity => AdjustmentType.CHANGE_IN_CAPACITY
          case EMRInstance.AdjustmentType.ExactCapacity => AdjustmentType.EXACT_CAPACITY
          case EMRInstance.AdjustmentType.PercentChangeInCapacity => AdjustmentType.PERCENT_CHANGE_IN_CAPACITY
        }

        val scalingPolicy = SimpleScalingPolicyConfiguration.builder()
          .adjustmentType(adjType)
          .scalingAdjustment(rule.scalingAdjustment)
          .coolDown(rule.coolDown)
          .build()

        val action = ScalingAction.builder()
            .simpleScalingPolicyConfiguration(scalingPolicy)
            .build()

        val cmpOp = rule.trigger.comparisonOperator match {
          case EMRInstance.ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.GREATER_THAN_OR_EQUAL
          case EMRInstance.ComparisonOperator.GreaterThan => ComparisonOperator.GREATER_THAN
          case EMRInstance.ComparisonOperator.LessThan => ComparisonOperator.LESS_THAN
          case EMRInstance.ComparisonOperator.LessThanOrEqual => ComparisonOperator.LESS_THAN_OR_EQUAL
        }
        val statistic = rule.trigger.statistic match {
          case EMRInstance.Statistic.SampleCount => Statistic.SAMPLE_COUNT
          case EMRInstance.Statistic.Average => Statistic.AVERAGE
          case EMRInstance.Statistic.Sum => Statistic.SUM
          case EMRInstance.Statistic.Minimum => Statistic.MINIMUM
          case EMRInstance.Statistic.Maximum => Statistic.MAXIMUM
        }

        val dimensions= rule.trigger.dimensions.map(d =>
          MetricDimension.builder().key(d.key).value(d.value).build()
        )

        val cloudWatchAlarmDefinition = CloudWatchAlarmDefinition.builder()
          .comparisonOperator(cmpOp)
          .evaluationPeriods(rule.trigger.evaluationPeriods)
          .metricName(rule.trigger.metricName)
          .namespace(rule.trigger.namespace)
          .period(rule.trigger.period)
          .threshold(rule.trigger.threshold)
          .statistic(statistic)
          .unit(rule.trigger.unit)
          .dimensions(dimensions.asJavaCollection)
          .build()


        val trigger = ScalingTrigger.builder()
            .cloudWatchAlarmDefinition(cloudWatchAlarmDefinition)
            .build()

        ScalingRule.builder()
          .name(rule.name)
          .description(rule.name)
          .action(action)
          .trigger(trigger)
          .build()
      }

      AutoScalingPolicy.builder()
        .constraints(ScalingConstraints.builder().maxCapacity(as.max).minCapacity(as.min).build())
        .rules(as.rules.map(mkRule).asJavaCollection)
        .build()
    }

    private def mkInstanceGroup(instance: EMRInstance.Instance): InstanceGroupConfig = {
      val roleType = instance.instanceGroupType match {
        case EMRInstance.InstanceGroupType.Core => InstanceRoleType.CORE
        case EMRInstance.InstanceGroupType.Master => InstanceRoleType.MASTER
        case EMRInstance.InstanceGroupType.Task => InstanceRoleType.TASK
      }
      val market = instance.market.fold(MarketType.ON_DEMAND)({
        case EMRInstance.Market.OnDemand => MarketType.ON_DEMAND
        case EMRInstance.Market.Spot => MarketType.SPOT
      })

      val base = InstanceGroupConfig.builder()
        .name(instance.name.getOrElse(s"Mist${instance.instanceGroupType}"))
        .instanceRole(roleType)
        .instanceType(instance.instanceType)
        .instanceCount(instance.instanceCount)
        .market(market)

      val withEbs = instance.ebs match {
        case Some(ebs) => base.ebsConfiguration(ebsConfiguration(ebs))
        case None => base
      }

      val withBidPrice = instance.bidPrice match {
        case Some(p) => withEbs.bidPrice(p)
        case None => withEbs
      }

      val result = instance.autoScaling match {
        case Some(as) => withBidPrice.autoScalingPolicy(mkAutoScaling(as))
        case None => withBidPrice
      }

      result.build()
    }

    private def mkInstancesConfig(builder: JFICBuilder, instances: Seq[EMRInstance.Instance]): JFICBuilder = {
      val groups = instances.map(i => mkInstanceGroup(i))
      builder.instanceGroups(groups.asJavaCollection)
    }

    override def start(settings: EMRRunSettings, instances: Seq[EMRInstance.Instance]): IO[EmrInfo] = {
      import settings._

      //TODO: configuration!
      val sparkApp = Application.builder().name("Spark").build()

      val initial = JobFlowInstancesConfig.builder()
      val addInstances = mkInstancesConfig(initial, instances)
      val instancesConfig = addInstances
        .keepJobFlowAliveWhenNoSteps(true)
        .ec2KeyName(keyPair)
        .ec2SubnetId(subnetId)
        .additionalMasterSecurityGroups(additionalGroup)
        .additionalSlaveSecurityGroups(additionalGroup)
        .build()

      val request = RunJobFlowRequest.builder()
        .name(s"mist-$name")
        .releaseLabel(releaseLabel)
        .applications(sparkApp)
        .jobFlowRole(emrEc2Role)
        .serviceRole(emrRole)
        .autoScalingRole(autoScalingRole)
        .instances(instancesConfig).build()

      for {
        resp <- orig.runJobFlow(request).toIO
        maybeSt <- status(resp.jobFlowId())
        out <- maybeSt match {
          case Some(info) => IO.pure(info)
          case None => IO.raiseError(new RuntimeException(s"Couldn't get infromation about cluster ${resp.jobFlowId()}"))
        }
      } yield out
    }

    override def status(id: String): IO[Option[EmrInfo]] = {
      val req = DescribeClusterRequest.builder().clusterId(id).build()
      orig.describeCluster(req).toIO
        .map(resp => Option(EmrInfo.fromCluster(resp.cluster())))
        .handleErrorWith({
          case _: EMRException => IO.pure(None)
          case e => IO.raiseError(e)
        })
    }

    override def stop(id: String): IO[Unit] = {
      val req = TerminateJobFlowsRequest.builder().jobFlowIds(id).build()
      orig.terminateJobFlows(req).toIO.map(_ => ())
    }
  }

  def create(accessKey: String, secretKey: String, region: String): EMRClient[IO] = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val reg = Region.of(region)
    val emrClient = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(reg)
      .build()

    new Default(emrClient)
  }
}

