package io.hydrosphere.mist.aws

import cats._
import cats.implicits._
import cats.effect.IO
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.ec2.model._

import scala.collection.JavaConverters._
import jFutureSyntax._

case class SecGroupData(
  vpcId: String,
  fromPort: Int,
  toPort: Int,
  cidrIp: String
)

case class InstanceData(
  ip: String,
  vpcId: String,
  subnetId: String
) {
  def cidrIp: String = ip + "/32"
}

trait EC2Service[F[_]] {

  def getInstanceData(id: String): F[Option[InstanceData]]
  
  def getSecGroup(name: String): F[Option[String]]
  def createSecGroup(name: String, descr: String, data: SecGroupData): F[String]

  def getOrCreateSecGroup(name: String, descr: String, data: SecGroupData)(implicit M: Monad[F]): F[String] = {
    for {
      curr <- getSecGroup(name)
      out <- curr match {
        case Some(_) => M.pure(name)
        case None => createSecGroup(name, descr, data)
      }
    } yield out
  }
}

object EC2Service {

  def fromSdk(ec2Client: EC2AsyncClient): EC2Service[IO] = {
    new EC2Service[IO] {

      override def getSecGroup(name: String): IO[Option[String]] = {
        val req = DescribeSecurityGroupsRequest.builder().groupNames(name).build()
        val io = ec2Client.describeSecurityGroups(req).toIO

        io.map(r => r.securityGroups().asScala.headOption.map(_.groupName()))
          .handleErrorWith(e => e match {
            case _: software.amazon.awssdk.services.ec2.model.EC2Exception => IO.pure(None)
            case _ => IO.raiseError(e)
          })
      }

      override def createSecGroup(
        name: String,
        descr: String,
        data: SecGroupData
      ): IO[String] = {

        def ingressReq(data: SecGroupData, groupId: String): AuthorizeSecurityGroupIngressRequest = {
          import data._

          AuthorizeSecurityGroupIngressRequest.builder()
            .groupId(groupId)
            .cidrIp(cidrIp)
            .fromPort(fromPort)
            .toPort(toPort)
            .ipProtocol("TCP")
            .build()
        }

        val createReq = CreateSecurityGroupRequest.builder()
          .groupName(name)
          .description(descr)
          .vpcId(data.vpcId)
          .build()

        for {
          resp <- ec2Client.createSecurityGroup(createReq).toIO
          groupId = resp.groupId()
          ingReq = ingressReq(data, groupId)
          _ <- ec2Client.authorizeSecurityGroupIngress(ingReq).toIO
        } yield groupId
      }

      override def getInstanceData(id: String): IO[Option[InstanceData]] = {
        def extractData(resp: DescribeInstancesResponse): Option[InstanceData] = {
          for {
            reservation <- resp.reservations().asScala.headOption
            instance <- reservation.instances().asScala.headOption
          } yield InstanceData(instance.privateIpAddress(), instance.vpcId(), instance.subnetId())
        }

        val req = DescribeInstancesRequest.builder().instanceIds(id).build()
        for {
          resp <- ec2Client.describeInstances(req).toIO
          data = extractData(resp)
        } yield data
      }
    }
  }

}
