package io.hydrosphere.mist.master.execution.aws

import cats._
import cats.implicits._
import cats.effect._
import io.hydrosphere.mist.master.execution.aws.JFutureSyntax._
import io.hydrosphere.mist.master.models.EMRInstances
import io.hydrosphere.mist.master.models.EMRInstances.{AutoScaling, Ebs, VolumeType}
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.{Unit => _, _}

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration

trait EMRClient[F[_]] {

  def start(settings: EMRRunSettings, instances: EMRInstances): F[EmrInfo]
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

//    private def mkAutoScaling(as: AutoScaling): AutoScalingPolicy = {
//      SimpleScalingPolicyConfiguration.builder()
//          .adjustmentType(AdjustmentType.)
//          .scalingAdjustment()
//
//      ScalingRule.builder()
//        .name()
//        .trigger(ScalingTrigger.builder().cloudWatchAlarmDefinition(CloudWatchAlarmDefinition.builder().metricName()))
//      val rules =
//      AutoScalingPolicy.builder()
//        .constraints(ScalingConstraints.builder().maxCapacity(as.max).minCapacity(as.min).build())
//        .rules()
//    }

    private def mkInstanceGroup(instance: EMRInstances.FleetInstance): InstanceGroupConfig = {
      val roleType = instance.instanceGroupType match {
        case EMRInstances.InstanceGroupType.Core => InstanceRoleType.CORE
        case EMRInstances.InstanceGroupType.Master => InstanceRoleType.MASTER
        case EMRInstances.InstanceGroupType.Task => InstanceRoleType.TASK
      }
      val market = instance.market match {
        case EMRInstances.Market.OnDemand => MarketType.ON_DEMAND
        case EMRInstances.Market.Spot => MarketType.SPOT
      }

      val base = InstanceGroupConfig.builder()
        .name(instance.name.getOrElse(s"Mist${instance.instanceGroupType}"))
        .instanceRole(roleType)
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

      withBidPrice.build()
    }

    private def mkInstancesConfig(builder: JFICBuilder, instances: EMRInstances): JFICBuilder = {
      instances match {
        case fixed: EMRInstances.Fixed =>
          builder.instanceCount(fixed.instanceCount)
            .masterInstanceType(fixed.masterInstanceType)
            .slaveInstanceType(fixed.slaveInstanceType)

        case fleets: EMRInstances.Fleets =>
          val groups = fleets.instances.map(i => mkInstanceGroup(i))
          builder.instanceGroups(groups.asJavaCollection)
      }
    }

    override def start(settings: EMRRunSettings, instances: EMRInstances): IO[EmrInfo] = {
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

