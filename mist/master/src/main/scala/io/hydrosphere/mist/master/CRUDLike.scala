package io.hydrosphere.mist.master

import io.hydrosphere.mist.master.data.ContextsStorage
import io.hydrosphere.mist.master.execution.ExecutionService
import io.hydrosphere.mist.master.interfaces.http.ContextCreateRequest
import io.hydrosphere.mist.master.models.ContextConfig
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

trait CRUDLike[A] {
  def update(a: A): Future[A]
  def getAll(): Future[Seq[A]]
  def get(id: String): Future[Option[A]]
  def delete(id: String): Future[Option[A]]
}

trait ContextsCRUDLike extends CRUDLike[ContextConfig] {
  def create(req: ContextCreateRequest): Future[ContextConfig]
}

trait ContextsCRUDMixin extends ContextsCRUDLike {
  val execution: ExecutionService
  val contextsStorage: ContextsStorage

  def update(ctx: ContextConfig): Future[ContextConfig] = for {
    upd <- contextsStorage.update(ctx)
    _ = execution.updateContext(upd)
  } yield upd


  def getAll(): Future[Seq[ContextConfig]] = contextsStorage.all
  def get(id: String): Future[Option[ContextConfig]] = contextsStorage.get(id)
  def delete(id: String): Future[Option[ContextConfig]] = contextsStorage.delete(id)
  def create(req: ContextCreateRequest): Future[ContextConfig] = {
    val ctx = req.toContextWithFallback(contextsStorage.defaultConfig)
    update(ctx)
  }
}
