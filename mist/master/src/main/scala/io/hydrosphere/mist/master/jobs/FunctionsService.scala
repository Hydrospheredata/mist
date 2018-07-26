package io.hydrosphere.mist.master.jobs

import java.io.File

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import cats.data._
import cats.implicits._
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.{ExtractedFunctionData, FunctionInfoData, PythonEntrySettings}
import io.hydrosphere.mist.master.artifact.ArtifactRepository
import io.hydrosphere.mist.master.data.{Contexts, ContextsStorage, FunctionConfigStorage}
import io.hydrosphere.mist.master.models.{ContextConfig, FunctionConfig}
import io.hydrosphere.mist.utils.Logger
import mist.api.data.JsMap

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

class FunctionsService(
  functionInfoActor: ActorRef,
  functionStorage: FunctionConfigStorage,
  ctxStorage: Contexts,
  artifactRepository: ArtifactRepository
)(implicit ec: ExecutionContext) extends Logger {

  val timeoutDuration = 5 seconds
  implicit val commonTimeout = Timeout(timeoutDuration)

  private def funcWithCtx(id: String): OptionT[Future, (FunctionConfig, ContextConfig)] = {
    for {
      f <- OptionT(functionStorage.get(id))
      ctx <- OptionT.liftF(ctxStorage.getOrDefault(f.defaultContext))
    } yield (f, ctx)
  }

  private def artifactForFunction(fn: FunctionConfig): Option[File] = {
    val out = artifactRepository.get(fn.path)
    if (out.isEmpty) {
      logger.warn(s"Function ${fn.name} wasn't created/removed properly. File ${fn.path} doesn't exists")
    }
    out
  }

  def hasFunction(id: String): Future[Boolean] = functionStorage.get(id).map(_.isDefined)

  def updateConfig(cfg: FunctionConfig): Future[FunctionConfig] = functionStorage.update(cfg)

  def update(cfg: FunctionConfig): Future[FunctionInfoData] = {
    for {
      fullInfo <- getFunctionInfoByConfig(cfg)
      _ <- updateConfig(cfg)
    } yield fullInfo
  }


  def getFunctionInfo(id: String): Future[Option[FunctionInfoData]] = {
    val f = for {
      (function, ctx) <- funcWithCtx(id)
      file     <- OptionT.fromOption[Future](artifactForFunction(function))
      data     <- OptionT.liftF(askInfoProvider[ExtractedFunctionData](createGetInfoMsg(function, ctx, file)))
      info     =  createJobInfoData(function, data)
    } yield info
    f.value
  }

  def getFunctionInfoByConfig(function: FunctionConfig): Future[FunctionInfoData] = {
    artifactForFunction(function) match {
      case Some(file) =>
        for {
          ctx <- ctxStorage.getOrDefault(function.defaultContext)
          data <- askInfoProvider[ExtractedFunctionData](createGetInfoMsg(function, ctx, file))
        } yield createJobInfoData(function, data)
      case None => Future.failed(new IllegalArgumentException(s"file should exists by path ${function.path}"))
    }
  }

  def validateFunctionParams(
    id: String,
    params: JsMap
  ): Future[Option[Unit]] = {
    val f = for {
      (function, ctx) <- funcWithCtx(id)
      file            <- OptionT.fromOption[Future](artifactForFunction(function))
      _ <- OptionT.liftF(askInfoProvider[Unit](createValidateParamsMsg(function, ctx, file, params)))
    } yield ()

    f.value
  }

  def validateFunctionParamsByConfig(function: FunctionConfig, params: JsMap): Future[Unit] = {
    artifactForFunction(function) match {
      case Some(file) =>
        for {
          ctx <- ctxStorage.getOrDefault(function.defaultContext)
          _ <- askInfoProvider[Unit](createValidateParamsMsg(function, ctx, file, params))
        } yield ()
      case None => Future.failed(new IllegalArgumentException(s"file not exists by path ${function.path}"))
    }
  }

  def allFunctions: Future[Seq[FunctionInfoData]] = {
    // TODO here we lose info about invalid configuration
    def toFunctionInfoRequest(f: FunctionConfig, ctx: ContextConfig): Option[GetFunctionInfo] = {
      artifactForFunction(f).map(file => createGetInfoMsg(f, ctx, file))
    }

    for {
      functions    <- functionStorage.all
      ctxs         <- ctxStorage.all
      functionsMap =  functions.map(e => e.name -> e).toMap
      data         <-
        if (functions.nonEmpty) {
          val ctxMap = ctxs.map(c => c.name -> c).toMap
          val paired = functions.map(f => f -> ctxMap.getOrElse(f.defaultContext, ctxStorage.defaultConfig))

          val requests = paired.flatMap({ case (f, c) => toFunctionInfoRequest(f, c)}).toList
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

  def delete(id: String): Future[Option[FunctionInfoData]] = {
    val out = for {
      fn <- OptionT(functionStorage.delete(id))
      data <- OptionT(askInfoProvider[Option[ExtractedFunctionData]](DeleteFunctionInfo(id)))
    } yield createJobInfoData(fn, data)

    out.value
  }

  private def askInfoProvider[T: ClassTag](msg: Any, t: Timeout): Future[T] =
    typedAsk[T](functionInfoActor, msg, t)

  private def typedAsk[T: ClassTag](ref: ActorRef, msg: Any, t: Timeout): Future[T] =
    ref.ask(msg)(t).mapTo[T]

  private def askInfoProvider[T: ClassTag](msg: Any): Future[T] =
    typedAsk[T](functionInfoActor, msg, commonTimeout)

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

  private def createGetInfoMsg(
    function: FunctionConfig,
    ctx: ContextConfig,
    file: File
  ): GetFunctionInfo = GetFunctionInfo(
    function.className,
    file.getAbsolutePath,
    function.name,
    EnvInfo(PythonEntrySettings.fromConf(ctx.sparkConf))
  )

  private def createValidateParamsMsg(
    function: FunctionConfig,
    ctx: ContextConfig,
    file: File,
    params: JsMap
  ): ValidateFunctionParameters = ValidateFunctionParameters(
    function.className,
    file.getAbsolutePath,
    function.name,
    params,
    EnvInfo(PythonEntrySettings.fromConf(ctx.sparkConf))
  )
}
