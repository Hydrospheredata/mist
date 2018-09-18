package io.hydrosphere.mist.aws

import cats._
import cats.effect.IO
import cats.implicits._
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.iam.IAMAsyncClient

case class SetupData(
  subnetId: String,
  securityGroupId: String,
  emrRole: String,
  ec2EmrRole: String
)

trait AwsSetup[F[_]] {

  def setup(instanceId: String): F[SetupData]

}

object AwsSetup {

  def default[F[_]](iam: IAMService[F], ec2: EC2Service[F])(implicit ME: MonadError[F, Throwable]): AwsSetup[F] = {
    new AwsSetup[F] {

      val ec2EmrRole = AWSRole("mist-EMREC2", "default emr ec2 role", AWSRoleData.EC2EMR)
      val emrRole = AWSRole("mist-EMR", "default emr role", AWSRoleData.EMR)
      val secGroupName = "mist-internal"
      val secGroupDecr = "Master-worker communications"

      override def setup(instanceId: String): F[SetupData] = {
        for {
          maybeInst <- ec2.getInstanceData(instanceId)
          data <- maybeInst match {
            case Some(d) => ME.pure(d)
            case None => ME.raiseError[InstanceData](new RuntimeException(s"Unknown instance: $instanceId"))
          }
          ec2EmrRole <- iam.getOrCreate(ec2EmrRole)
          emrRole <- iam.getOrCreate(emrRole)

          secGroupData = SecGroupData(data.vpcId, 0, 65535, data.cidrIp)
          secGroupId <- ec2.getOrCreateSecGroup(secGroupName, secGroupDecr, secGroupData)
        } yield SetupData(data.subnetId, secGroupId, emrRole.name, ec2EmrRole.name)
      }
    }
  }

  def create(accessKey: String, secretKey: String, regionName: String): AwsSetup[IO] = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)

    val ec2Client = EC2AsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(regionName))
      .build()

    val iamClient = IAMAsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.AWS_GLOBAL)
      .build()

    val iam = IAMService.fromSdk(iamClient)
    val ec2 = EC2Service.fromSdk(ec2Client)
    default(iam, ec2)
  }
}

