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
import mist.api.data.JsLikeMap

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class FunctionInfoService(
  functionInfoActor: ActorRef,
  functionStorage: FunctionConfigStorage,
  artifactRepository: ArtifactRepository
)(implicit ec: ExecutionContext) {
  val timeoutDuration = 5 seconds
  implicit val commonTimeout = Timeout(timeoutDuration)

  def getFunctionInfo(id: String): Future[Option[FunctionInfoData]] = {
    val f = for {
      function <- OptionT(functionStorage.get(id))
      file     <- OptionT.fromOption[Future](artifactRepository.get(function.path))
      data     <- OptionT.liftF(askInfoProvider[ExtractedFunctionData](createGetInfoMsg(function, file)))
      info     =  createJobInfoData(function, data)
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
    params: JsLikeMap
  ): Future[Option[Unit]] = {
    val f = for {
      function   <- OptionT(functionStorage.get(id))
      file       <- OptionT.fromOption[Future](artifactRepository.get(function.path))
      _ <- OptionT.liftF(askInfoProvider[Unit](createValidateParamsMsg(function, file, params)))
    } yield ()

    f.value
  }

  def validateFunctionParamsByConfig(function: FunctionConfig, params: JsLikeMap): Future[Unit] = {
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
      functions    <- functionStorage.all
      functionsMap =  functions.map(e => e.name -> e).toMap
      data         <-
        if (functions.nonEmpty) {
          val requests = functions.flatMap(toFunctionInfoRequest).toList
          val timeout = Timeout(timeoutDuration * requests.size.toLong)
          askInfoProvider[Seq[ExtractedFunctionData]](GetAllFunctions(requests), timeout)
        } else
          Future.successful(Seq.empty)
    } yield {
      data.flatMap(d => {
        functionsMap.get(d.name).map { ep => createJobInfoData(ep, d)}
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
    params: JsLikeMap
  ): ValidateFunctionParameters = ValidateFunctionParameters(
    function.className,
    file.getAbsolutePath,
    function.name,
    params
  )
}
