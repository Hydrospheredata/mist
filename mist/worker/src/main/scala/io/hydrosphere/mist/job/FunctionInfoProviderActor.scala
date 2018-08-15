package io.hydrosphere.mist.job

import java.io.File

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.ExtractedFunctionData
import io.hydrosphere.mist.utils.{Err, Succ, TryLoad}
import mist.api.{Extracted, Failed}
import mist.api.data.JsMap

import scala.concurrent.duration._
import scala.util.Failure


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

  def keys: Seq[K]

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

  override def keys: Seq[K] = cache.keys.toSeq
}


class FunctionInfoProviderActor(
  jobInfoExtractor: FunctionInfoExtractor,
  ttl: FiniteDuration
) extends Actor with ActorLogging {

  type StateCache = Cache[CacheKey, TryLoad[FunctionInfo]]

  implicit val ec = context.dispatcher

  override def supervisorStrategy: SupervisorStrategy = OneForOneStrategy(){
    case e: Throwable =>
      log.error(e, "Handled error")
      Resume
  }

  context.system.scheduler.schedule(ttl, ttl, self, EvictCache)

  override def receive: Receive = cached(Cache[CacheKey, TryLoad[FunctionInfo]](ttl))

  private def wrapError(e: Throwable): Throwable = e match {
    case e: Error => new RuntimeException(e)
    case e: Throwable => e
  }

  def cached(cache: StateCache): Receive = {
    case d: DeleteFunctionInfo =>
      val key = cache.keys.find(k => k.name == d.name)
      val value = key.flatMap(k => cache.get(k))

      val next = key match {
        case Some(k) => cache - k
        case None => cache
      }
      val rsp = value.flatMap({
        case Succ(info) => Some(info.data)
        case Err(e) => None
      })
      context become cached(next)
      sender() ! rsp

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
        def buildError(f: Failed): String = {
          f match {
            case Failed.InternalError(msg) => s"Internal error: $msg"
            case Failed.InvalidField(name, f) => s"Invalid field $name:" + buildError(f)
            case Failed.InvalidValue(msg) => s"Invalid value: $msg"
            case Failed.InvalidType(expected, got) => s"Invalid type: expected $expected, got $got"
            case Failed.ComplexFailure(failures) => failures.map(buildError).mkString("Errors[", ",", "]")
            case Failed.IncompleteObject(clazz, failure: Failed) => s"Incomplete object for class $clazz, reason: ${buildError(failure)}"
          }
        }

        inst.validateParams(p) match {
          case Extracted(_) => Succ(())
          case f: Failed => Err(new IllegalArgumentException(buildError(f)))
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
      val key = mkKey(req, file)
      cache.getOrUpdate(key)(_ => jobInfo(req))
    } else cache -> Err(new IllegalArgumentException(s"File should exists in path ${req.jobPath}"))
  }

  private def jobInfo(req: InfoRequest): TryLoad[FunctionInfo] = {
    import req._
    val f = new File(jobPath)
    jobInfoExtractor.extractInfo(f, className, req.envInfo).map(e => e.copy(data = e.data.copy(name = req.name)))
  }

  case class CacheKey(
    name: String,
    className: String,
    lastModified: Long,
    env: EnvInfo
  )

  private def mkKey(req: InfoRequest, file: File): CacheKey =
    CacheKey(
      name = req.name,
      className = req.className,
      lastModified = file.lastModified(),
      env = req.envInfo
    )

}

object FunctionInfoProviderActor {

  def props(jobInfoExtractor: FunctionInfoExtractor, ttl: FiniteDuration = 3600 seconds): Props =
    Props(new FunctionInfoProviderActor(jobInfoExtractor, ttl))

}

