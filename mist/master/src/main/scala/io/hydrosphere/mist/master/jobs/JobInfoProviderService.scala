package io.hydrosphere.mist.master.jobs

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{Action, GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.models.EndpointConfig

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.reflect.ClassTag

class JobInfoProviderService(
  jobInfoProvider: ActorRef,
  endpointStorage: EndpointsStorage,
  artifactRepository: ArtifactRepository
)(implicit ec: ExecutionContext) {
  implicit val timeout = Timeout(5 seconds)

  def getJobInfo(id: String): Future[Option[JobInfoData]] = {
    val f = for {
      endpoint <- OptionT(endpointStorage.get(id))
      file     <- OptionT.fromOption[Future](artifactRepository.get(endpoint.path))
      jobInfo  <- OptionT.liftF(askInfoProvider[JobInfoData](GetJobInfo(endpoint.className, file.getAbsolutePath)))
    } yield jobInfo.copy(
      defaultContext = endpoint.defaultContext,
      path = endpoint.path,
      name = endpoint.name
    )

    f.value
  }

  def getJobInfoByConfig(endpoint: EndpointConfig): Future[JobInfoData] = {
    artifactRepository.get(endpoint.path) match {
      case Some(file) =>
        askInfoProvider[JobInfoData](GetJobInfo(endpoint.className, file.getAbsolutePath))
          .map { _.copy(
              defaultContext = endpoint.defaultContext,
              name = endpoint.name,
              path=endpoint.path
            )
          }
      case None => Future.failed(new IllegalArgumentException(s"file should exists by path ${endpoint.path}"))
    }
  }

  def validateJob(
    id: String,
    params: Map[String, Any],
    action: Action
  ): Future[Option[Unit]] = {
    val f = for {
      endpoint   <- OptionT(endpointStorage.get(id))
      file       <- OptionT.fromOption[Future](artifactRepository.get(endpoint.path))
      _ <- OptionT.liftF(askInfoProvider[Unit](ValidateJobParameters(
        endpoint.className, file.getAbsolutePath, action, params
      )))
    } yield ()

    f.value
  }

  def validateJobByConfig(endpoint: EndpointConfig, params: Map[String, Any], action: Action): Future[Unit] = {
    artifactRepository.get(endpoint.path) match {
      case Some(file) =>
        askInfoProvider[Unit](ValidateJobParameters(
          endpoint.className, file.getAbsolutePath, action, params
        ))
      case None => Future.failed(new IllegalArgumentException(s"file not exists by path ${endpoint.path}"))
    }

  }

  private def askInfoProvider[T: ClassTag](msg: Any): Future[T] = typedAsk[T](jobInfoProvider, msg)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] = ref.ask(msg).mapTo[T]

}
