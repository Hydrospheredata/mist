package io.hydrosphere.mist.master.execution.workers.emr

import java.util.function.BiConsumer

import io.hydrosphere.mist.utils.Scheduling
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.{Application, ClusterState, ClusterStatus, DescribeClusterRequest, DescribeStepRequest, JobFlowInstancesConfig, RunJobFlowRequest, RunJobFlowResponse, TerminateJobFlowsRequest}

import scala.concurrent.{Future, Promise}
import io.hydrosphere.mist.utils.jFutureSyntax._

import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration

case class StartClusterSettings(
  name: String,
  keyPair: String,
  releaseLabel: String,
  masterInstanceType: String,
  slaveInstanceType: String,
  instanceCount: Int,
  subnetId: String
)

case class ClusterFuture(
  id: String,
  await: Future[Unit]
)

trait AwsEMRClient {

  def start(settings: StartClusterSettings): Future[String]

  def status(id: String): Future[Unit]

  def stop(id: String): Future[Unit]

}

object AwsEMRClient {

  private val scheduling = Scheduling.stpBased(2)

  class Wrapper(
    orig: EMRAsyncClient,
    scheduling: Scheduling
  ) extends AwsEMRClient {

    override def start(settings: StartClusterSettings): Future[String] = {
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

    override def status(id: String): Future[ClusterStatus] = {
      val req = DescribeClusterRequest.builder().clusterId(id).build()
      orig.describeCluster(req).toFuture.map(resp => resp.cluster().status())
    }

    def awaitStarted(id: String, tick: FiniteDuration, atMost: FiniteDuration): Future[Unit] = {

      def loop(id: String, await: FiniteDuration, tries: Int): Future[Unit] = {
        status(id).flatMap(s => s.state() match {
          case ClusterState.WAITING => Future.successful(())
          case ClusterState.STARTING | ClusterState.BOOTSTRAPPING =>
            if ( tries == 0 ) {
              Future.failed(new RuntimeException(s"Cluster $id wasn't started for $atMost"))
            } else {
              scheduling.delay(await).flatMap(_ => loop(id, await, tries - 1))
            }
          case ClusterState.TERMINATED | ClusterState.TERMINATED_WITH_ERRORS | ClusterState.TERMINATING =>
            Future.failed(new RuntimeException(s"Cluster $id is terminated"))
          case ClusterState.RUNNING => Future.failed(new RuntimeException(s"Cluster ${id} in running state"))
        })
      }

      val maxTimes = (atMost / tick).floor.toInt
      loop(id, tick, maxTimes)
    }
  }

  def create(
    accessKey: String,
    secretKey: String,
    region: String
  ): AwsEMRClient = {
    val provider = StaticCredentialsProvider.create(new AwsCredentials(accessKey, secretKey))
    val client = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(region))
      .build()

    new Wrapper(client)
  }
}

