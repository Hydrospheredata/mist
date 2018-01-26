package io.hydrosphere.mist.master.jobs

import java.io.File

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{GetAllJobInfo, GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.core.jvmjob.{ExtractedData, JobInfoData}
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.EndpointsStorage
import io.hydrosphere.mist.master.models.EndpointConfig

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class JobInfoProviderService(
  jobInfoProvider: ActorRef,
  endpointStorage: EndpointsStorage,
  artifactRepository: ArtifactRepository
)(implicit ec: ExecutionContext) {
  val timeoutDuration = 5 seconds
  implicit val commonTimeout = Timeout(timeoutDuration)

  def getJobInfo(id: String): Future[Option[JobInfoData]] = {
    val f = for {
      endpoint <- OptionT(endpointStorage.get(id))
      file     <- OptionT.fromOption[Future](artifactRepository.get(endpoint.path))
      data     <- OptionT.liftF(askInfoProvider[ExtractedData](createGetInfoMsg(endpoint, file)))
      info     =  createJobInfoData(endpoint, data)
    } yield info
    f.value
  }

  def getJobInfoByConfig(endpoint: EndpointConfig): Future[JobInfoData] = {
    artifactRepository.get(endpoint.path) match {
      case Some(file) =>
        askInfoProvider[ExtractedData](createGetInfoMsg(endpoint, file))
          .map(data => createJobInfoData(endpoint, data))
      case None => Future.failed(new IllegalArgumentException(s"file should exists by path ${endpoint.path}"))
    }
  }

  def validateJob(
    id: String,
    params: Map[String, Any]
  ): Future[Option[Unit]] = {
    val f = for {
      endpoint   <- OptionT(endpointStorage.get(id))
      file       <- OptionT.fromOption[Future](artifactRepository.get(endpoint.path))
      _ <- OptionT.liftF(askInfoProvider[Unit](createValidateParamsMsg(endpoint, file, params)))
    } yield ()

    f.value
  }

  def validateJobByConfig(endpoint: EndpointConfig, params: Map[String, Any]): Future[Unit] = {
    artifactRepository.get(endpoint.path) match {
      case Some(file) =>
        askInfoProvider[Unit](createValidateParamsMsg(endpoint, file, params))
      case None => Future.failed(new IllegalArgumentException(s"file not exists by path ${endpoint.path}"))
    }
  }

  def allJobInfos: Future[Seq[JobInfoData]] = {
    def toJobInfoRequest(e: EndpointConfig): Option[GetJobInfo] = {
      artifactRepository.get(e.path)
        .map(f => createGetInfoMsg(e, f))
    }
    for {
      endpoints   <- endpointStorage.all
      enpointsMap =  endpoints.map(e => e.name -> e).toMap
      data        <-
        if (endpoints.nonEmpty) {
          val requests = endpoints.flatMap(toJobInfoRequest).toList
          val timeout = Timeout(timeoutDuration * requests.size.toLong)
          askInfoProvider[Seq[ExtractedData]](GetAllJobInfo(requests), timeout)
        } else
          Future.successful(Seq.empty)
    } yield {
      data.flatMap(d => {
        enpointsMap.get(d.name).map { ep => createJobInfoData(ep, d)}
      })
    }
  }

  private def askInfoProvider[T: ClassTag](msg: Any, t: Timeout): Future[T] =
    typedAsk[T](jobInfoProvider, msg, t)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any, t: Timeout): Future[T] =
    ref.ask(msg)(t).mapTo[T]

  private def askInfoProvider[T: ClassTag](msg: Any): Future[T] =
    typedAsk[T](jobInfoProvider, msg)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] =
    ref.ask(msg).mapTo[T]

  private def createJobInfoData(endpoint: EndpointConfig, data: ExtractedData): JobInfoData = JobInfoData(
    endpoint.name,
    endpoint.path,
    endpoint.className,
    endpoint.defaultContext,
    data.lang,
    data.execute,
    data.isServe,
    data.tags
  )

  private def createGetInfoMsg(endpoint: EndpointConfig, file: File): GetJobInfo = GetJobInfo(
    endpoint.className,
    file.getAbsolutePath,
    endpoint.name
  )

  private def createValidateParamsMsg(
    endpoint: EndpointConfig,
    file: File,
    params: Map[String, Any]
  ): ValidateJobParameters = ValidateJobParameters(
    endpoint.className,
    file.getAbsolutePath,
    endpoint.name,
    params
  )
}
