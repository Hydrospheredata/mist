package io.hydrosphere.mist.agent

import software.amazon.awssdk.services.emr.EMRAsyncClient
import software.amazon.awssdk.services.emr.model.TerminateJobFlowsRequest

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import io.hydrosphere.mist.utils.CFConversion._
import software.amazon.awssdk.auth.credentials.{AwsCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region

class EMRTermination(client: EMRAsyncClient) {

  def terminate(id: String): Future[Unit] = {
    val req = TerminateJobFlowsRequest.builder().jobFlowIds(id).build()
    client.terminateJobFlows(req).toFuture.map(_ => ())
  }

}

object EMRTermination {

  def create(accessKey: String, secretKey: String, region: String): EMRTermination = {
    val credentials = AwsCredentials.create(accessKey, secretKey)
    val provider = StaticCredentialsProvider.create(credentials)
    val reg = Region.of(region)
    val emrClient = EMRAsyncClient.builder()
      .credentialsProvider(provider)
      .region(reg)
      .build()

    new EMRTermination(emrClient)
  }
}
