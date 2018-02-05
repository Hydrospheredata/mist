package io.hydrosphere.mist.master.jobs

import java.io.File

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData.{GetAllFunctions, GetFunctionInfo, ValidateFunctionParameters}
import io.hydrosphere.mist.core.jvmjob.{ExtractedFunctionData, FunctionInfoData}
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.FunctionConfigStorage
import io.hydrosphere.mist.master.models.FunctionConfig

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class FunctionInfoService(
  functionInfoActor: ActorRef,
  endpointStorage: FunctionConfigStorage,
  artifactRepository: ArtifactRepository
)(implicit ec: ExecutionContext) {
  val timeoutDuration = 5 seconds
  implicit val commonTimeout = Timeout(timeoutDuration)

  def getFunctionInfo(id: String): Future[Option[FunctionInfoData]] = {
    val f = for {
      endpoint <- OptionT(endpointStorage.get(id))
      file     <- OptionT.fromOption[Future](artifactRepository.get(endpoint.path))
      data     <- OptionT.liftF(askInfoProvider[ExtractedFunctionData](createGetInfoMsg(endpoint, file)))
      info     =  createJobInfoData(endpoint, data)
    } yield info
    f.value
  }

  def getFunctionInfoByConfig(function: FunctionConfig): Future[FunctionInfoData] = {
    artifactRepository.get(function.path) match {
      case Some(file) =>
        askInfoProvider[ExtractedFunctionData](createGetInfoMsg(function, file))
          .map(data => createJobInfoData(function, data))
      case None => Future.failed(new IllegalArgumentException(s"file should exists by path ${function.path}"))
    }
  }

  def validateFunctionParams(
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

  def validateFunctionParamsByConfig(function: FunctionConfig, params: Map[String, Any]): Future[Unit] = {
    artifactRepository.get(function.path) match {
      case Some(file) =>
        askInfoProvider[Unit](createValidateParamsMsg(function, file, params))
      case None => Future.failed(new IllegalArgumentException(s"file not exists by path ${function.path}"))
    }
  }

  def allFunctions: Future[Seq[FunctionInfoData]] = {
    def toFunctionInfoRequest(f: FunctionConfig): Option[GetFunctionInfo] = {
      artifactRepository.get(f.path)
        .map(file => createGetInfoMsg(f, file))
    }
    for {
      endpoints   <- endpointStorage.all
      enpointsMap =  endpoints.map(e => e.name -> e).toMap
      data        <-
        if (endpoints.nonEmpty) {
          val requests = endpoints.flatMap(toFunctionInfoRequest).toList
          val timeout = Timeout(timeoutDuration * requests.size.toLong)
          askInfoProvider[Seq[ExtractedFunctionData]](GetAllFunctions(requests), timeout)
        } else
          Future.successful(Seq.empty)
    } yield {
      data.flatMap(d => {
        enpointsMap.get(d.name).map { ep => createJobInfoData(ep, d)}
      })
    }
  }

  private def askInfoProvider[T: ClassTag](msg: Any, t: Timeout): Future[T] =
    typedAsk[T](functionInfoActor, msg, t)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any, t: Timeout): Future[T] =
    ref.ask(msg)(t).mapTo[T]

  private def askInfoProvider[T: ClassTag](msg: Any): Future[T] =
    typedAsk[T](functionInfoActor, msg)
  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any): Future[T] =
    ref.ask(msg).mapTo[T]

  private def createJobInfoData(function: FunctionConfig, data: ExtractedFunctionData): FunctionInfoData = FunctionInfoData(
    function.name,
    function.path,
    function.className,
    function.defaultContext,
    data.lang,
    data.execute,
    data.isServe,
    data.tags
  )

  private def createGetInfoMsg(function: FunctionConfig, file: File): GetFunctionInfo = GetFunctionInfo(
    function.className,
    file.getAbsolutePath,
    function.name
  )

  private def createValidateParamsMsg(
    function: FunctionConfig,
    file: File,
    params: Map[String, Any]
  ): ValidateFunctionParameters = ValidateFunctionParameters(
    function.className,
    file.getAbsolutePath,
    function.name,
    params
  )
}
