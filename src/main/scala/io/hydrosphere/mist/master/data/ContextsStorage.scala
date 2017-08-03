package io.hydrosphere.mist.master.data

import java.util.concurrent.Executors

import io.hydrosphere.mist.master.ContextsSettings
import io.hydrosphere.mist.master.models.ContextConfig

import scala.concurrent.{Future, ExecutionContext}

class ContextsStorage(
  fsStorage: FsStorage[ContextConfig],
  defaultSettings: ContextsSettings
)(implicit ex: ExecutionContext) {

  private val allFromSettings = defaultSettings.contexts.values.toSeq :+ defaultSettings.default

  def get(name: String): Future[Option[ContextConfig]] = {
    defaultSettings.contexts.get(name) match {
      case None => Future { fsStorage.entry(name) }
      case s @ Some(_) => Future.successful(s)
    }
    Future { fsStorage.entry(name) }
  }

  def getOrDefault(name: String): Future[ContextConfig] =
    get(name).map(_.getOrElse(defaultSettings.default))

  def all: Future[Seq[ContextConfig]] =
    Future { fsStorage.entries } map (stored => stored ++ allFromSettings)

  def precreated: Future[Seq[ContextConfig]] = all.map(_.filter(_.precreated))

  def update(config: ContextConfig): Future[ContextConfig] = {
    Future { fsStorage.write(config.name, config) }
  }
}

object ContextsStorage {

  def create(path:String, settings: ContextsSettings): ContextsStorage = {
    val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(3))
    val fsStorage = new FsStorage(checkDirectory(path), ConfigRepr.ContextConfigRepr)
    new ContextsStorage(fsStorage, settings)(ec)
  }
}

