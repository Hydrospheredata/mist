package io.hydrosphere.mist.master.data

import java.util.concurrent.Executors

import io.hydrosphere.mist.master.ContextsSettings
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, ExecutionContext}

class ContextsStorage(
  fsStorage: FsStorage[ContextConfig],
  defaultSettings: ContextsSettings
)(implicit ex: ExecutionContext) {

  private val defaultsMap = {
    import defaultSettings._
    contexts + (default.name -> default)
  }
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
}

object ContextsStorage {

  val DefaultKey = "default"

  def create(path:String, settings: ContextsSettings): ContextsStorage = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))
    val fsStorage = new FsStorage(checkDirectory(path), ConfigRepr.ContextConfigRepr)
    new ContextsStorage(fsStorage, settings)(ec)
  }
}

