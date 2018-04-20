package io.hydrosphere.mist.job

import java.io.File

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.jvmjob.ExtractedFunctionData
import io.hydrosphere.mist.utils.{Err, Succ, TryLoad}
import mist.api.data.JsMap
import mist.api.internal.BaseFunctionInstance

import scala.concurrent.duration._


trait Cache[K, V] {
  self =>
  def get(k: K): Option[V]

  def getOrUpdate(k: K)(f: K => V): (Cache[K, V], V)

  def put(k: K, v: V): Cache[K, V]

  def size: Int

  def removeItem(k: K): Cache[K, V]

  def evictAll: Cache[K, V]

  def +(e: (K, V)): Cache[K, V] = self.put(e._1, e._2)

  def -(k: K): Cache[K, V] = self.removeItem(k)

  protected def now: Long = System.currentTimeMillis()

}

object Cache {
  def apply[K, V](
    ttl: Duration,
    cache: Map[K, (Long, V)] = Map.empty[K, (Long, V)]
  ): Cache[K, V] =
    new TTLCache[K, V](ttl, cache)

}

case class TTLCache[K, V](
  ttl: Duration,
  cache: Map[K, (Long, V)]
) extends Cache[K, V] {

  override def get(k: K): Option[V] =
    cache.get(k)
      .filter { case (expiration, _) => expiration > now }
      .map(_._2)

  override def getOrUpdate(k: K)(f: K => V): (Cache[K, V], V) = {
    get(k) match {
      case Some(v) => (this, v)
      case None =>
        val v = f(k)
        (put(k, v), v)
    }
  }

  override def put(k: K, v: V): Cache[K, V] = {
    val newCache = cache + (k -> (now + ttl.toMillis, v))
    this.copy(cache = newCache)
  }

  override def size: Int = cache.size

  override def removeItem(k: K): Cache[K, V] =
    this.copy(cache = cache - k)

  override def evictAll: Cache[K, V] = {
    val newCache = cache.collect {
      case (k, entry@(expiration, _)) if expiration > now => k -> entry
    }
    this.copy(cache = newCache)
  }
}


class FunctionInfoProviderActor(
  jobInfoExtractor: FunctionInfoExtractor,
  ttl: FiniteDuration
) extends Actor with ActorLogging {

  type StateCache = Cache[String, TryLoad[FunctionInfo]]

  implicit val ec = context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
    case e: Throwable =>
      log.error(e, "Handled error")
      Resume
  }

  context.system.scheduler.schedule(ttl, ttl, self, EvictCache)

  override def receive: Receive = cached(Cache[String, TryLoad[FunctionInfo]](ttl))

  private def wrapError(e: Throwable): Throwable = e match {
    case e: Error => new RuntimeException(e)
    case e: Throwable => e
  }

  def cached(cache: StateCache): Receive = {
    case r: GetFunctionInfo =>
      val (next, v) = usingCache(cache, r)
      val rsp = v match {
        case Succ(info) => info.data
        case Err(e) =>
          log.info(s"Responding with err on {}: {} {}", r, e.getClass, e.getMessage)
          Status.Failure(wrapError(e))
      }
      sender() ! rsp
      context become cached(next)

    case req: ValidateFunctionParameters =>
      def validate(inst: BaseFunctionInstance, p: JsMap): TryLoad[Unit] = {
        inst.validateParams(p) match {
          case Some(err) => Err(err)
          case None => Succ(())
        }
      }
      val (next, v) = usingCache(cache, req)
      val rsp = v.flatMap(i => validate(i.instance, req.params)) match {
        case Succ(_) => Status.Success(())
        case Err(e) =>
          log.info(s"Responding with err on {}: {} {}", req, e.getClass, e.getMessage)
          Status.Failure(wrapError(e))
      }
      sender() ! rsp
      context become cached(next)

    case GetAllFunctions(requests) =>
      //TODO send errors to master
      val (next, results) = requests.foldLeft((cache, Seq.empty[ExtractedFunctionData])){
        case ((cache, acc), req) =>
          val (upd, v) = usingCache(cache, req)
          val nextAcc = v match {
            case Succ(info) =>
              acc :+ info.data
            case Err(err) =>
              log.error(err, err.getMessage)
              acc
          }
          (upd, nextAcc)
      }
      sender() ! results
      context become cached(next)

    case GetCacheSize =>
      sender() ! cache.size

    case EvictCache =>
      context become cached(cache.evictAll)
  }

  private def usingCache(cache: StateCache, req: InfoRequest): (StateCache, TryLoad[FunctionInfo]) = {
    val file = new File(req.jobPath)
    if (file.exists() && file.isFile) {
      val key = cacheKey(req, file)
      cache.getOrUpdate(key)(_ => jobInfo(req))
    } else cache -> Err(new IllegalArgumentException(s"File should exists in path ${req.jobPath}"))
  }

  private def jobInfo(req: InfoRequest): TryLoad[FunctionInfo] = {
    import req._
    val f = new File(jobPath)
    jobInfoExtractor.extractInfo(f, className).map(e => e.copy(data = e.data.copy(name = req.name)))
  }

  private def cacheKey(req: InfoRequest, file: File): String = {
    s"${req.name}_${req.className}_${file.lastModified()}"
  }

}

object FunctionInfoProviderActor {

  def props(jobInfoExtractor: FunctionInfoExtractor, ttl: FiniteDuration = 3600 seconds): Props =
    Props(new FunctionInfoProviderActor(jobInfoExtractor, ttl))

}

