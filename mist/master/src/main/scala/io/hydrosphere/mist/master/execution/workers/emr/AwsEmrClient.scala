package io.hydrosphere.mist.master.execution.workers.emr

import io.hydrosphere.mist.utils.jFutureSyntax._
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
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
  case class Started(masterDnsName: String) extends EMRStatus
  case object Terminated extends EMRStatus
  case object Terminating extends EMRStatus

  def fromCluster(cluster: Cluster): EMRStatus =  cluster.status().state() match {
    case ClusterState.STARTING | ClusterState.BOOTSTRAPPING => Starting
    case ClusterState.WAITING | ClusterState.RUNNING => Started(cluster.masterPublicDnsName())
    case ClusterState.TERMINATED | ClusterState.TERMINATED_WITH_ERRORS => Terminated
    case ClusterState.TERMINATING => Terminating
    case ClusterState.UNKNOWN_TO_SDK_VERSION => throw new RuntimeException("Used amazon sdk version is incompatible with aws")
  }
}

trait AwsEMRClient {

  def start(settings: EMRRunSettings): Future[String]

  def status(id: String): Future[EMRStatus]

  def stop(id: String): Future[Unit]

}

object AwsEMRClient {

  class Wrapper(orig: EMRAsyncClient) extends AwsEMRClient {

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

      orig.runJobFlow(request).toFuture.map(_.jobFlowId())
    }

    override def stop(id: String): Future[Unit] = {
      val req = TerminateJobFlowsRequest.builder().jobFlowIds(id).build()
      orig.terminateJobFlows(req).toFuture.map(_ => ())
    }

    override def status(id: String): Future[EMRStatus] = {
      val req = DescribeClusterRequest.builder().clusterId(id).build()
      for {
        resp <- orig.describeCluster(req).toFuture
        status = EMRStatus.fromCluster(resp.cluster())
      } yield status
    }

  }

  def create(
    accessKey: String,
    secretKey: String,
    region: String
  ): AwsEMRClient = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val client = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(region))
      .build()

    new Wrapper(client)
  }
}

