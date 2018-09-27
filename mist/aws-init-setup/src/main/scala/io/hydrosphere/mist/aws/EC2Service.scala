package io.hydrosphere.mist.aws

import cats._
import cats.implicits._
import cats.effect.IO
import software.amazon.awssdk.services.ec2.EC2AsyncClient
import software.amazon.awssdk.services.ec2.model._

import scala.collection.JavaConverters._
import JFutureSyntax._

sealed trait IngressAddr
object IngressAddr {
  final case class CidrIP(value: String) extends IngressAddr
  final case class Group(value: SecGroup) extends IngressAddr
}

case class IngressData(
  fromPort: Int,
  toPort: Int,
  address: IngressAddr,
  protocol: String
)

case class SecGroup(id: String, name: String)

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

  def getSecGroup(name: String): F[Option[SecGroup]]
  def createSecGroup(name: String, descr: String, vpcId: String, data: IngressData): F[SecGroup]

  def getOrCreateSecGroup(name: String, descr: String, vpcId: String, data: IngressData)(implicit M: Monad[F]): F[SecGroup] = {
    for {
      curr <- getSecGroup(name)
      out <- curr match {
        case Some(v) => M.pure(v)
        case None => createSecGroup(name, descr, vpcId, data)
      }
    } yield out
  }

}

object EC2Service {

  def fromSdk(ec2Client: EC2AsyncClient): EC2Service[IO] = {
    new EC2Service[IO] {

      override def addIngressRule(groupId: String, data: IngressData): IO[Unit] = {
        println(s"HERE: $groupId $data")
        import data._
        val ipPremission = IpPermission.builder()
          .fromPort(fromPort)
          .toPort(toPort)
          .ipProtocol(protocol)

        val withAddr = address match {
          case IngressAddr.CidrIP(v) =>
            val range = IpRange.builder().cidrIp(v).build()
            ipPremission.ipv4Ranges(range)
          case IngressAddr.Group(v) =>
            val group = UserIdGroupPair.builder()
              .groupId(v.id)
              .groupName(v.name)
              .description(s"${v.id}:${v.name}")
              .build()
            ipPremission.userIdGroupPairs(group)
        }

        val req = AuthorizeSecurityGroupIngressRequest.builder()
          .groupId(groupId)
          .ipPermissions(withAddr.build())
          .build()

        println(req)
        ec2Client.authorizeSecurityGroupIngress(req).toIO.map(r => {println(r)})
      }

      override def getSecGroup(name: String): IO[Option[SecGroup]] = {
        val req = DescribeSecurityGroupsRequest.builder().groupNames(name).build()
        val io = ec2Client.describeSecurityGroups(req).toIO

        io.map(r => r.securityGroups().asScala.headOption.map(s => SecGroup(s.groupId(), s.groupName())))
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
      ): IO[SecGroup] = {

        val createReq = CreateSecurityGroupRequest.builder()
          .groupName(name)
          .description(descr)
          .vpcId(vpcId)
          .build()

        for {
          resp <- ec2Client.createSecurityGroup(createReq).toIO
          groupId = resp.groupId()
          _ <- addIngressRule(groupId, data)
        } yield SecGroup(resp.groupId, name)
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
