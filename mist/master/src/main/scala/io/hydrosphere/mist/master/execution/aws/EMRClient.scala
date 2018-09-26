package io.hydrosphere.mist.master.execution.aws

import cats._
import cats.implicits._
import cats.effect._
import io.hydrosphere.mist.master.execution.aws.JFutureSyntax._
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.{Unit => _, _}

import scala.concurrent.duration.FiniteDuration

trait EMRClient[F[_]] {

  def start(settings: EMRRunSettings): F[EmrInfo]
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

    override def start(settings: EMRRunSettings): IO[EmrInfo] = {
      import settings._

      //TODO: configuration!
      val sparkApp = Application.builder().name("Spark").build()
      val request = RunJobFlowRequest.builder()
        .name(s"mist-$name")
        .releaseLabel(releaseLabel)
        .applications(sparkApp)
        .jobFlowRole(emrEc2Role)
        .serviceRole(emrRole)
        .instances(JobFlowInstancesConfig.builder()
          .keepJobFlowAliveWhenNoSteps(true)
          .ec2KeyName(keyPair)
          .instanceCount(instanceCount)
          .masterInstanceType(masterInstanceType)
          .slaveInstanceType(slaveInstanceType)
          .ec2SubnetId(subnetId)
          .additionalMasterSecurityGroups(additionalGroup)
          .additionalSlaveSecurityGroups(additionalGroup)
          .build()
        ).build()

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

