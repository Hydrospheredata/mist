package io.hydrosphere.mist.aws

import java.util.concurrent.CompletableFuture
import java.util.function.BiConsumer

import cats._
import cats.implicits._
import cats.Monad
import cats.effect._
import software.amazon.awssdk.services.iam.IAMAsyncClient
import software.amazon.awssdk.services.iam.model.{AttachRolePolicyRequest, CreateRoleRequest, GetRoleRequest, Role}

import JFutureSyntax._

import scala.io.Source

case class AWSRoleData(
  trustPolicyJson: String,
  permissionsArn: String
)

object AWSRoleData {

  private def readResourceJson(name: String): String = {
    val stream = getClass.getResourceAsStream(name)
    Source.fromInputStream(stream).mkString.replace("\n", "")
  }

  val EMR = AWSRoleData(
    readResourceJson("/trustEMR.json"),
    "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
  )

  val EC2EMR = AWSRoleData(
    readResourceJson("/trustEC2EMR.json"),
    permissionsArn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
  )

}

case class AWSRole(
  name: String,
  description: String,
  data: AWSRoleData
)

trait IAMService[F[_]] {

  def createRole(role: AWSRole): F[AWSRole]
  def getRole(name: String): F[Option[String]]

  def getOrCreate(role: AWSRole)(implicit m: Monad[F]): F[AWSRole] = {
    for {
      out <- getRole(role.name)
      role <- out match {
        case Some(_) => m.pure(role)
        case None => createRole(role)
      }
    } yield role
  }
}

object IAMService {

  def fromSdk(iamClient: IAMAsyncClient): IAMService[IO] =
    new IAMService[IO] {

      override def getRole(name: String): IO[Option[String]] = {
        val req = GetRoleRequest.builder().roleName(name).build()

        iamClient.getRole(req).toIO
          .map(r => Option(r.role().roleName()))
          .handleErrorWith({
            case _: software.amazon.awssdk.services.iam.model.NoSuchEntityException => IO.pure(None)
            case e => IO.raiseError(e)
          })
      }

      override def createRole(role: AWSRole): IO[AWSRole] = {
        val createReq = CreateRoleRequest.builder()
          .roleName(role.name)
          .description(role.description)
          .assumeRolePolicyDocument(role.data.trustPolicyJson)
          .build()

        val attachReq = AttachRolePolicyRequest.builder()
          .policyArn(role.data.permissionsArn)
          .roleName(role.name)
          .build()

        for {
          _ <- iamClient.createRole(createReq).toIO
          _ <- iamClient.attachRolePolicy(attachReq).toIO
        } yield role
      }
  }
}
