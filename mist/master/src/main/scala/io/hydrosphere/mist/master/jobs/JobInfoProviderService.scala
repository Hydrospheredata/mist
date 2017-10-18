package io.hydrosphere.mist.master.jobs

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{Action, GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.models.EndpointConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

class JobInfoProviderService(
  jobInfoProvider: ActorRef,
  endpointStorage: EndpointsStorage
)(implicit ec: ExecutionContext) {
  implicit val timeout = Timeout(5 seconds)

  def getJobInfo(id: String): Future[Option[FullJobInfo]] = {
    val f = for {
      endpoint <- OptionT(endpointStorage.get(id))
      jobInfo  <- OptionT.liftF(askInfoProvider[FullJobInfo](GetJobInfo(endpoint.className, endpoint.path)))
    } yield jobInfo.copy(defaultContext = endpoint.defaultContext)

    f.value
  }

  def getJobInfo(endpoint: EndpointConfig): Future[FullJobInfo] = {
    for {
      info     <- askInfoProvider[FullJobInfo](GetJobInfo(endpoint.className, endpoint.path))
      fullInfo =  info.copy(defaultContext = endpoint.defaultContext, name = endpoint.name)
    } yield fullInfo
  }

  def validateJob(
    id: String,
    params: Map[String, Any],
    action: Action
  ): Future[Option[Unit]] = {
    val f = for {
      endpoint   <- OptionT(endpointStorage.get(id))
      validated  <- OptionT.liftF(askInfoProvider[Unit](ValidateJobParameters(
        endpoint.className, endpoint.path, action, params
      )))
    } yield validated

    f.value
  }

  def validateJob(endpoint: EndpointConfig, params: Map[String, Any], action: Action): Future[Unit] = {
    askInfoProvider[Unit](ValidateJobParameters(
      endpoint.className, endpoint.path, action, params
    ))
  }

  private def askInfoProvider[T: ClassTag](msg: Any): Future[T] = typedAsk[T](jobInfoProvider, msg)

  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] = ref.ask(msg).mapTo[T]

}
