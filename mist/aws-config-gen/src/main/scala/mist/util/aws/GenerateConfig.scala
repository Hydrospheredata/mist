package mist.util.aws

import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.ec2.model._
import software.amazon.awssdk.services.emr.EMRAsyncClient

import scala.concurrent.{Await, Future}
import scala.io.StdIn
import scala.sys.process._
import io.hydrosphere.mist.utils.jFutureSyntax._
import software.amazon.awssdk.services.emr.model.DescribeClusterRequest
import software.amazon.awssdk.services.iam.IAMAsyncClient
import software.amazon.awssdk.services.iam.model._

import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global


class AwsClient(
  ec2Client: EC2AsyncClient,
  emrClient: EMRAsyncClient,
  iamClient: IAMAsyncClient
) {

  def getInstance(id: String): Future[Instance] = {
    val req = DescribeInstancesRequest.builder().instanceIds(id).build()
    for {
      resp <- ec2Client.describeInstances(req).toFuture
    } yield resp.reservations().get(0).instances().get(0)
  }

  def checkSecGroup(secGroupName: String): Future[Option[SecurityGroup]] = {
    println("chec sec group")
    val req = DescribeSecurityGroupsRequest.builder().groupNames(secGroupName).build()
    ec2Client.describeSecurityGroups(req).toFuture
      .map(_.securityGroups().asScala.headOption)
      .recover({
        case _: software.amazon.awssdk.services.ec2.model.EC2Exception => None
      })
  }

  def createMistSecGroup(
    name: String,
    vpcId: String,
    description: String
  ): Future[String] = {
    println("create sec group")
    val req = CreateSecurityGroupRequest.builder()
      .groupName(name)
      .description(description)
      .vpcId(vpcId)
      .build()

    for {
      resp <- ec2Client.createSecurityGroup(req).toFuture
    } yield resp.groupId()
  }

  def authorizeSecGroupIngress(groupId: String, cidr: String): Future[Unit] = {
    println("utohirize")
    val req = AuthorizeSecurityGroupIngressRequest.builder()
      .fromPort(0)
      .toPort(65535)
      .groupId(groupId)
      .cidrIp(cidr)
      .ipProtocol("TCP")
      .build()

    ec2Client.authorizeSecurityGroupIngress(req).toFuture.map(_ => ())
  }

  def createGroupFully(
    secGroupName: String,
    description: String,
    cidr: String,
    vpcId: String
  ): Future[String] = {
    println("create fulllygorup")
    for {
      id <- createMistSecGroup(secGroupName, vpcId, description)
      _ <- authorizeSecGroupIngress(id, cidr)
    } yield id
  }

  def getOrCreateSecGroup(
    secGroupName: String,
    description: String,
    cidr: String,
    vpcId: String
  ): Future[String] = {
    println("get or create sec group")
    for {
      maybe <- checkSecGroup(secGroupName)
      id <- maybe match {
        case Some(group) => Future.successful(group.groupId())
        case None => createGroupFully(secGroupName, description, cidr, vpcId)
      }
    } yield id
  }

  def createRole(
    name: String,
    trustPolicyJson: String,
    permissionsPolicyJson: String,
    description: String
  ): Future[Role] = {
    println("create role")
    val createRole = CreateRoleRequest.builder()
      .assumeRolePolicyDocument(trustPolicyJson)
      .roleName(name)
      .description(description)
      .build()

    val putPerms = PutRolePolicyRequest.builder().roleName(name)
      .policyDocument(permissionsPolicyJson)
      .policyName(name + "_policy")
      .build()

    for {
      createResp <- iamClient.createRole(createRole).toFuture
      role = createResp.role()
      _ <- iamClient.putRolePolicy(putPerms).toFuture
    } yield role
  }

  def checkRole(name: String): Future[Option[Role]] = {
    println("check role")

    val req = GetRoleRequest.builder().roleName(name).build()
    iamClient.getRole(req).toFuture
      .map(resp => Option(resp.role()))
      .recover({
        case _: software.amazon.awssdk.services.iam.model.NoSuchEntityException => None
      })
  }

  def getOrCreateRole(
    name: String,
    trustPolicyJson: String,
    permPolicyJson: String,
    description: String): Future[Role] = {
    println("get or create role")
    for {
      maybe <- checkRole(name)
      out <- maybe match {
        case Some(role) => Future.successful(role)
        case None => createRole(name, trustPolicyJson, permPolicyJson, description)
      }
    } yield out
  }
}

case class SetupData(
  subnetId: String,
  emrMasterRole: String,
  emrSlaveRole: String,
  additionalSecurityGroup: String
)

class MistAwsSetup(client: AwsClient, reportF: String => Unit ) {

  val emrSlavePolicy = "AmazonElasticMapReduceRole"
  val emrSlavePolicyArn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"

  val emrSlaveRoleDescription = "Allows Elastic MapReduce to call AWS services such as EC2 on your behalf."
  val emrMasterPolicy = "AmazonElasticMapReduceforEC2Role"
  val emrMasterPolicyArn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
  val emrMasterRoleDescription = "Allows EC2 instances in an Elastic MapReduce cluster to call AWS services such as S3 on your behalf."

  val secGroupDescription = "Access emr resources from mist master instance"

  val emrTrustPolicy =
    """
      |{
      |  "Version": "2008-10-17",
      |  "Statement": [
      |    {
      |      "Sid": "",
      |      "Effect": "Allow",
      |      "Principal": {
      |        "Service": [
      |          "elasticmapreduce.amazonaws.com"
      |        ]
      |      },
      |      "Action": "sts:AssumeRole"
      |    }
      |  ]
      |}
    """.stripMargin

  val emrPermsPolicy =
    """
      |
      |    "Version": "2012-10-17",
      |    "Statement": [
      |        {
      |            "Effect": "Allow",
      |            "Resource": "*",
      |            "Action": [
      |                "ec2:AuthorizeSecurityGroupEgress",
      |                "ec2:AuthorizeSecurityGroupIngress",
      |                "ec2:CancelSpotInstanceRequests",
      |                "ec2:CreateNetworkInterface",
      |                "ec2:CreateSecurityGroup",
      |                "ec2:CreateTags",
      |                "ec2:DeleteNetworkInterface",
      |                "ec2:DeleteSecurityGroup",
      |                "ec2:DeleteTags",
      |                "ec2:DescribeAvailabilityZones",
      |                "ec2:DescribeAccountAttributes",
      |                "ec2:DescribeDhcpOptions",
      |                "ec2:DescribeImages",
      |                "ec2:DescribeInstanceStatus",
      |                "ec2:DescribeInstances",
      |                "ec2:DescribeKeyPairs",
      |                "ec2:DescribeNetworkAcls",
      |                "ec2:DescribeNetworkInterfaces",
      |                "ec2:DescribePrefixLists",
      |                "ec2:DescribeRouteTables",
      |                "ec2:DescribeSecurityGroups",
      |                "ec2:DescribeSpotInstanceRequests",
      |                "ec2:DescribeSpotPriceHistory",
      |                "ec2:DescribeSubnets",
      |                "ec2:DescribeTags",
      |                "ec2:DescribeVpcAttribute",
      |                "ec2:DescribeVpcEndpoints",
      |                "ec2:DescribeVpcEndpointServices",
      |                "ec2:DescribeVpcs",
      |                "ec2:DetachNetworkInterface",
      |                "ec2:ModifyImageAttribute",
      |                "ec2:ModifyInstanceAttribute",
      |                "ec2:RequestSpotInstances",
      |                "ec2:RevokeSecurityGroupEgress",
      |                "ec2:RunInstances",
      |                "ec2:TerminateInstances",
      |                "ec2:DeleteVolume",
      |                "ec2:DescribeVolumeStatus",
      |                "ec2:DescribeVolumes",
      |                "ec2:DetachVolume",
      |                "iam:GetRole",
      |                "iam:GetRolePolicy",
      |                "iam:ListInstanceProfiles",
      |                "iam:ListRolePolicies",
      |                "iam:PassRole",
      |                "s3:CreateBucket",
      |                "s3:Get*",
      |                "s3:List*",
      |                "sdb:BatchPutAttributes",
      |                "sdb:Select",
      |                "sqs:CreateQueue",
      |                "sqs:Delete*",
      |                "sqs:GetQueue*",
      |                "sqs:PurgeQueue",
      |                "sqs:ReceiveMessage",
      |                "cloudwatch:PutMetricAlarm",
      |                "cloudwatch:DescribeAlarms",
      |                "cloudwatch:DeleteAlarms",
      |                "application-autoscaling:RegisterScalableTarget",
      |                "application-autoscaling:DeregisterScalableTarget",
      |                "application-autoscaling:PutScalingPolicy",
      |                "application-autoscaling:DeleteScalingPolicy",
      |                "application-autoscaling:Describe*"
      |            ]
      |        },
      |        {
      |            "Effect": "Allow",
      |            "Action": "iam:CreateServiceLinkedRole",
      |            "Resource": "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot*",
      |            "Condition": {
      |                "StringLike": {
      |                    "iam:AWSServiceName": "spot.amazonaws.com"
      |                }
      |            }
      |        }
      |    ]
      |}
    """.stripMargin

  val emrec2TrustPolicy =
    """
      | {
      |  "Version": "2008-10-17",
      |  "Statement": [
      |    {
      |      "Sid": "",
      |      "Effect": "Allow",
      |      "Principal": {
      |        "Service": [
      |          "ec2.amazonaws.com"
      |        ]
      |      },
      |      "Action": "sts:AssumeRole"
      |    }
      |  ]
      |}
    """.stripMargin

  val emrec2PermsPolicy =
    """
      |{
      |    "Version": "2012-10-17",
      |    "Statement": [{
      |        "Effect": "Allow",
      |        "Resource": "*",
      |        "Action": [
      |            "cloudwatch:*",
      |            "dynamodb:*",
      |            "ec2:Describe*",
      |            "elasticmapreduce:Describe*",
      |            "elasticmapreduce:ListBootstrapActions",
      |            "elasticmapreduce:ListClusters",
      |            "elasticmapreduce:ListInstanceGroups",
      |            "elasticmapreduce:ListInstances",
      |            "elasticmapreduce:ListSteps",
      |            "kinesis:CreateStream",
      |            "kinesis:DeleteStream",
      |            "kinesis:DescribeStream",
      |            "kinesis:GetRecords",
      |            "kinesis:GetShardIterator",
      |            "kinesis:MergeShards",
      |            "kinesis:PutRecord",
      |            "kinesis:SplitShard",
      |            "rds:Describe*",
      |            "s3:*",
      |            "sdb:*",
      |            "sns:*",
      |            "sqs:*",
      |            "glue:CreateDatabase",
      |            "glue:UpdateDatabase",
      |            "glue:DeleteDatabase",
      |            "glue:GetDatabase",
      |            "glue:GetDatabases",
      |            "glue:CreateTable",
      |            "glue:UpdateTable",
      |            "glue:DeleteTable",
      |            "glue:GetTable",
      |            "glue:GetTables",
      |            "glue:GetTableVersions",
      |            "glue:CreatePartition",
      |            "glue:BatchCreatePartition",
      |            "glue:UpdatePartition",
      |            "glue:DeletePartition",
      |            "glue:BatchDeletePartition",
      |            "glue:GetPartition",
      |            "glue:GetPartitions",
      |            "glue:BatchGetPartition",
      |            "glue:CreateUserDefinedFunction",
      |            "glue:UpdateUserDefinedFunction",
      |            "glue:DeleteUserDefinedFunction",
      |            "glue:GetUserDefinedFunction",
      |            "glue:GetUserDefinedFunctions"
      |        ]
      |    }]
      |}
    """.stripMargin

  def reported[A](f: => Future[A])(pre: => String) = {
    reportF(pre)
    f
  }

  def setup(instanceId: String): Future[SetupData] = {
    val secGroupName = s"mist_${instanceId}_sec_group"
    val emrMasterRoleName = s"mist_${instanceId}_EMR_EC2_DefaultRole"
    val emrSlaveRoleName = s"mist_${instanceId}_EMR_DefaultRole"

    import client._
    for {
      inst <- reported(getInstance(instanceId))(s"Getting instance $instanceId")
      _ = println(inst)
      _ <- reported(getOrCreateRole(emrSlaveRoleName, emrTrustPolicy.trim.replace("\n", ""), emrPermsPolicy.trim.replace("\n", ""), emrSlaveRoleDescription))(s"Check or create $emrSlaveRoleName role")
      _ <- reported(getOrCreateRole(emrMasterRoleName, emrec2TrustPolicy.trim.replace("\n", ""), emrec2PermsPolicy.trim.replace("\n", ""), emrMasterRoleDescription))(s"Check or create $emrMasterRoleName role")
      secGroupId <- {
        val cidr = inst.privateIpAddress() + "/32"
        reported(getOrCreateSecGroup(secGroupName, secGroupDescription, cidr, inst.vpcId()))(s"Check or create security group $secGroupName for cird: $cidr")
      }
    } yield SetupData(inst.subnetId(), emrMasterRoleName, emrSlaveRoleName, secGroupId)
  }

}

object MistAwsSetup {

  def apply(
    accessKey: String,
    secretKey: String,
    regionName: String,
    report: String => Unit
  ): MistAwsSetup = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val ec2lient = EC2AsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(regionName))
      .build()

    val iamClient = IAMAsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.AWS_GLOBAL)
      .build()

    val emrClient = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(regionName))
      .build()

    new MistAwsSetup(new AwsClient(ec2lient, emrClient, iamClient), report)
  }
}

object Test extends App {

  val setup = MistAwsSetup(
    regionName = "eu-central-1",
    println
  )

  println(Await.result(setup.setup("i-0be63dc079b0984ca"), Duration.Inf))
}

object GenerateConfig {

  def main(args: Array[String]): Unit = {
    val id = getInstanceId()

    val accessKey = StdIn.readLine("AWS Access Key ID:")
    val secretKey = StdIn.readLine("AWS Secret Access Key:")
    val regionName = StdIn.readLine("Default region name:")

    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val client = EC2AsyncClient.builder()
      .credentialsProvider(provider)
      .region(Region.of(regionName))
      .build()

    val req = DescribeInstancesRequest.builder().instanceIds(id).build()

    val out = Await.result(client.describeInstances(req).toFuture, Duration.Inf)
  }

  def getInstanceId(): String = {
    val out = "ec-metadata -i".!!
    out.replaceAll("instance-id: ", "").replaceAll("\n", "")
  }
}
