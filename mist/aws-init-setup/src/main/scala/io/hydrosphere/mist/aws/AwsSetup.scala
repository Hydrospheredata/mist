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
  ec2EmrRole: String,
  sshKeyPairName: String
)

trait AwsSetup[F[_]] {

  def setup(instanceId: String, sshKey: String): F[SetupData]

}

object AwsSetup {

  def default[F[_]](iam: IAMService[F], ec2: EC2Service[F])(implicit ME: MonadError[F, Throwable]): AwsSetup[F] = {
    new AwsSetup[F] {

      val ec2EmrRole = AWSRole("mist-EMREC2", "default emr ec2 role", AWSRoleData.EC2EMR)
      val emrRole = AWSRole("mist-EMR", "default emr role", AWSRoleData.EMR)
      val secGroupDecr = "Master-worker communications"

      def secGroupName(id: String): String = s"mist-internal-$id"
      def keyName(id: String): String = s"mist-$id"

      override def setup(instanceId: String, sshKey: String): F[SetupData] = {
        for {
          maybeInst <- ec2.getInstanceData(instanceId)
          data <- maybeInst match {
            case Some(d) => ME.pure(d)
            case None => ME.raiseError[InstanceData](new RuntimeException(s"Unknown instance: $instanceId"))
          }
          // analog of `aws emr create-default-roles`
          // for ec2 it's required to create InstanceProfile linked with Role
          // for emr it's enough to have only role
          ec2EmrRole <- iam.getOrCreateRole(ec2EmrRole)
          ec2EmrInstanceProfile <- iam.getOrCreateInstanceProfile(ec2EmrRole.name, ec2EmrRole.name)
          emrRole <- iam.getOrCreateRole(emrRole)

          // additional security group for emr
          // allows ingress traffic from mist-master
          secGroupData = IngressData(0, 65535, IngressAddr.CidrIP(data.cidrIp), "TCP")
          scGroupName = secGroupName(instanceId)
          internalSecGroup <- ec2.getOrCreateSecGroup(scGroupName, secGroupDecr, data.vpcId, secGroupData)

          // add ingress rule to mist-master node security group
          // allows ingress traffic from emr cluster
          // as source uses security group created above
          secGroupId  = internalSecGroup.id
          _ <- ec2.addIngressRule(data.secGroupIds.head, IngressData(0, 65535, IngressAddr.Group(internalSecGroup), "TCP"))

          keyName <- ec2.getOrCreateKeyPair(keyName(instanceId), sshKey)
        } yield SetupData(data.subnetId, secGroupId, emrRole.name, ec2EmrRole.name, keyName)
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
