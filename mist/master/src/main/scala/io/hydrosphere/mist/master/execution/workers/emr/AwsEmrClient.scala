package io.hydrosphere.mist.master.execution.workers.emr

import io.hydrosphere.mist.utils.jFutureSyntax._
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.ec2.model.AuthorizeSecurityGroupIngressRequest
import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.{Application, Cluster, ClusterState, DescribeClusterRequest, JobFlowInstancesConfig, RunJobFlowRequest, TerminateJobFlowsRequest}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class EMRRunSettings(
  name: String,
  keyPair: String,
  releaseLabel: String,
  masterInstanceType: String,
  slaveInstanceType: String,
  instanceCount: Int,
  subnetId: String
)

sealed trait EMRStatus
object EMRStatus {

  case object Starting extends EMRStatus
  case class Started(masterDnsName: String, secGroup: String) extends EMRStatus
  case object Terminated extends EMRStatus
  case object Terminating extends EMRStatus

  def fromCluster(cluster: Cluster): EMRStatus =  cluster.status().state() match {
    case ClusterState.STARTING | ClusterState.BOOTSTRAPPING => Starting
    case ClusterState.WAITING | ClusterState.RUNNING =>
      Started(cluster.masterPublicDnsName(), cluster.ec2InstanceAttributes().emrManagedMasterSecurityGroup())
    case ClusterState.TERMINATED | ClusterState.TERMINATED_WITH_ERRORS => Terminated
    case ClusterState.TERMINATING => Terminating
    case ClusterState.UNKNOWN_TO_SDK_VERSION => throw new RuntimeException("Used amazon sdk version is incompatible with aws")
  }
}

trait AwsEMRClient {

  def start(settings: EMRRunSettings): Future[String]

  def status(id: String): Future[EMRStatus]

  def stop(id: String): Future[Unit]

  def allowIngress(cidr: String, secGroup: String): Future[Unit]
}

object AwsEMRClient {

  class Wrapper(
    origEc2: EC2AsyncClient,
    origEmr: EMRAsyncClient
  ) extends AwsEMRClient {

    override def start(settings: EMRRunSettings): Future[String] = {
      import settings._

      val sparkApp = Application.builder().name("Spark").build()
      val request = RunJobFlowRequest.builder()
        .name(s"mist-$name")
        .releaseLabel(releaseLabel)
        .applications(sparkApp)
        .jobFlowRole("EMR_EC2_DefaultRole")
        .serviceRole("EMR_DefaultRole")
        .instances(JobFlowInstancesConfig.builder()
          .keepJobFlowAliveWhenNoSteps(true)
          .ec2KeyName(keyPair)
          .instanceCount(instanceCount)
          .masterInstanceType(masterInstanceType)
          .slaveInstanceType(slaveInstanceType)
          .ec2SubnetId(subnetId)
          .build()
        ).build()

      origEmr.runJobFlow(request).toFuture.map(_.jobFlowId())
    }

    override def stop(id: String): Future[Unit] = {
      val req = TerminateJobFlowsRequest.builder().jobFlowIds(id).build()
      origEmr.terminateJobFlows(req).toFuture.map(_ => ())
    }

    override def status(id: String): Future[EMRStatus] = {
      val req = DescribeClusterRequest.builder().clusterId(id).build()
      for {
        resp <- origEmr.describeCluster(req).toFuture
        status = EMRStatus.fromCluster(resp.cluster())
      } yield status
    }

    override def allowIngress(cidr: String, secGroup: String): Future[Unit] = {
      val auth = AuthorizeSecurityGroupIngressRequest.builder()
        .fromPort(0)
        .toPort(65535)
        .groupId(secGroup)
        .cidrIp(cidr)
        .ipProtocol("TCP")
        .build()

      origEc2.authorizeSecurityGroupIngress(auth).toFuture.map(_ => ())
    }

  }

  def create(
    accessKey: String,
    secretKey: String,
    region: String
  ): AwsEMRClient = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val reg = Region.of(region)
    val emrClient = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(reg)
      .build()

    val ec2Client = EC2AsyncClient.builder()
      .credentialsProvider(provider)
      .region(reg)
      .build()

    new Wrapper(ec2Client, emrClient)
  }
}

