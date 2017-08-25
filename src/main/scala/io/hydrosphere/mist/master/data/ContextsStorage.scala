package io.hydrosphere.mist.master.data

import java.util.concurrent.Executors

import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{ExecutionContext, Future}

class ContextsStorage(
  fsStorage: FsStorage[ContextConfig],
  defaults: ContextDefaults
)(implicit ex: ExecutionContext) {

  import defaults._

  def get(name: String): Future[Option[ContextConfig]] = {
    Future { fsStorage.entry(name) }.flatMap({
      case v @ Some(_) => Future.successful(v)
      case None => Future.successful(defaultsMap.get(name))
    })
  }

  def getOrDefault(name: String): Future[ContextConfig] =
    get(name).map(_.getOrElse(defaultSettings.default.copy(name = name)))

  def all: Future[Seq[ContextConfig]] = {
    Future { fsStorage.entries } map (stored => {
      val merged = defaultsMap ++ stored.map(a => a.name -> a).toMap
      merged.values.toSeq
    })
  }

  def precreated: Future[Seq[ContextConfig]] = all.map(_.filter(_.precreated))

  def update(config: ContextConfig): Future[ContextConfig] = {
    if (config.name == ContextsStorage.DefaultKey)
      Future.failed(new IllegalArgumentException("Can not create context with name:\"default\""))
    else
      Future { fsStorage.write(config.name, config) }
  }

  def defaultConfig: ContextConfig = defaults.defaultConfig
}

object ContextsStorage {

  val DefaultKey = "default"

  def create(path:String, mistConfigPath: String): ContextsStorage = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))
    val fsStorage = new FsStorage(checkDirectory(path), ConfigRepr.ContextConfigRepr)
    new ContextsStorage(fsStorage, new ContextDefaults(mistConfigPath))(ec)
  }
}

