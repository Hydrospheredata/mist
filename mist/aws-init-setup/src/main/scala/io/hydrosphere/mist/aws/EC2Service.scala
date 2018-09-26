package io.hydrosphere.mist.aws

import cats._
import cats.implicits._
import cats.effect.IO
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.ec2.model._

import scala.collection.JavaConverters._
import JFutureSyntax._

case class IngressData(
  fromPort: Int,
  toPort: Int,
  cidrIp: String,
  protocol: String
)

case class InstanceData(
  ip: String,
  vpcId: String,
  subnetId: String,
  secGroupIds: Seq[String]
) {
  def cidrIp: String = ip + "/32"
}

trait EC2Service[F[_]] {

  def getInstanceData(id: String): F[Option[InstanceData]]

  def getKeyPair(name: String): F[Option[String]]
  def createKeyPair(name: String, key: String): F[String]

  def addIngressRule(groupId: String, data: IngressData): F[Unit]

  def getOrCreateKeyPair(name: String, key: String)(implicit M: Monad[F]): F[String] = {
    for {
      curr <- getKeyPair(name)
      out <- curr match {
        case Some(_) => M.pure(name)
        case None => createKeyPair(name, key)
      }
    } yield out
  }

  def getSecGroup(name: String): F[Option[String]]
  def createSecGroup(name: String, descr: String, vpcId: String, data: IngressData): F[String]

  def getOrCreateSecGroup(name: String, descr: String, vpcId: String, data: IngressData)(implicit M: Monad[F]): F[String] = {
    for {
      curr <- getSecGroup(name)
      out <- curr match {
        case Some(_) => M.pure(name)
        case None => createSecGroup(name, descr, vpcId, data)
      }
    } yield out
  }

}

object EC2Service {

  def fromSdk(ec2Client: EC2AsyncClient): EC2Service[IO] = {
    new EC2Service[IO] {

      override def addIngressRule(groupId: String, data: IngressData): IO[Unit] = {
        import data._
        val req = AuthorizeSecurityGroupIngressRequest.builder()
          .groupId(groupId)
          .cidrIp(cidrIp)
          .fromPort(fromPort)
          .toPort(toPort)
          .ipProtocol(data.protocol)
          .build()
        ec2Client.authorizeSecurityGroupIngress(req).toIO.map(_ => ())
      }

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
        vpcId: String,
        data: IngressData
      ): IO[String] = {

        val createReq = CreateSecurityGroupRequest.builder()
          .groupName(name)
          .description(descr)
          .vpcId(vpcId)
          .build()

        for {
          resp <- ec2Client.createSecurityGroup(createReq).toIO
          groupId = resp.groupId()
          _ <- addIngressRule(groupId, data)
        } yield groupId
      }

      override def getInstanceData(id: String): IO[Option[InstanceData]] = {
        def extractData(resp: DescribeInstancesResponse): Option[InstanceData] = {
          for {
            reservation <- resp.reservations().asScala.headOption
            instance <- reservation.instances().asScala.headOption
          } yield {
            val secGroupsIds = instance.securityGroups().asScala.map(sg => sg.groupId())
            InstanceData(instance.privateIpAddress(), instance.vpcId(), instance.subnetId(), secGroupsIds)
          }
        }

        val req = DescribeInstancesRequest.builder().instanceIds(id).build()
        for {
          resp <- ec2Client.describeInstances(req).toIO
          data = extractData(resp)
        } yield data
      }

      override def getKeyPair(name: String): IO[Option[String]] = {
        val req = DescribeKeyPairsRequest.builder().keyNames(name).build()
        ec2Client.describeKeyPairs(req).toIO
          .map(resp => resp.keyPairs().asScala.headOption.map(i => i.keyName()))
          .handleErrorWith(e => e match {
            case _: software.amazon.awssdk.services.ec2.model.EC2Exception => IO.pure(None)
            case _ => IO.raiseError(e)
          })
      }

      override def createKeyPair(name: String, key: String): IO[String] = {
        val req = ImportKeyPairRequest.builder().keyName(name).publicKeyMaterial(key).build()
        ec2Client.importKeyPair(req).toIO.map(resp => resp.keyName())
      }

    }

  }

}
