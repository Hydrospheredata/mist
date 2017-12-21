package io.hydrosphere.mist.job

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.jvmjob.JobInfoData
import org.apache.commons.codec.digest.DigestUtils

import scala.concurrent.duration._
import scala.util.{Failure, Success}


trait Cache[K, V] { self =>
  def get(k: K): Option[V]
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


class JobInfoProviderActor(
  jobInfoExtractor: JobInfoExtractor,
  ttl: FiniteDuration
) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  context.system.scheduler.schedule(ttl, ttl, self, EvictCache)

  override def receive: Receive = cached(Cache[String, JobInfo](ttl))

  def cached(cache: Cache[String, JobInfo]): Receive = {
    case r: GetJobInfo =>
      jobInfoFromCache(cache, r) match {
        case Right(entry@(_, i)) =>
          sender() ! i.data
          context become cached(cache + entry)
        case Left(err) =>
          sender() ! Status.Failure(err)
      }

    case req: ValidateJobParameters =>
      import req._
      val message = jobInfoFromCache(cache, req) match {
        case Right(entry@(_, item)) => item.instance.validateParams(params) match {
          case Right(_) =>
            context become cached(cache + entry)
            Status.Success(())
          case Left(err) => Status.Failure(err)
        }
        case Left(err) => Status.Failure(err)
      }
      sender() ! message

    case GetAllJobInfo(requests) =>
      val jobInfoDatas = requests
        .map(req => jobInfoFromCache(cache, req))
        .foldLeft(Seq.empty[(String, JobInfo)]) {
          case (acc, Right(item)) => acc :+ item
          case (acc, Left(err)) =>
            log.error(err, err.getMessage)
            acc
        }

      sender() ! jobInfoDatas.map(_._2.data)

      val updatedCache = jobInfoDatas.foldLeft(cache) {
        case (c, entry) => c + entry
      }

      context become cached(updatedCache)

    case GetCacheSize =>
      sender() ! cache.size

    case EvictCache =>
      context become cached(cache.evictAll)

  }

  private def jobInfoFromCache(
    cache: Cache[String, JobInfo],
    req: InfoRequest
  ): Either[Throwable, (String, JobInfo)] = {
    val file = new File(req.jobPath)
    if (file.exists() && file.isFile) {
      val key = cacheKey(req)
      cache.get(key) match {
        case Some(info) => Right((key, info))
        case None => jobInfo(req) match {
          case Right(info) => Right((key, info))
          case Left(err) => Left(err)
        }
      }
    } else Left(new IllegalArgumentException(s"File should exists in path ${req.jobPath}"))
  }

  private def jobInfo(req: InfoRequest): Either[Throwable, JobInfo] = {
    import req._
    jobInfoExtractor.extractInfo(new File(jobPath), className) match {
      case Success(info) =>
        Right(info.copy(data = info.data.copy(
          defaultContext = req.defaultContext,
          name=req.name,
          path=req.originalPath
        )))
      case Failure(ex) =>
        Left(ex)
    }
  }

  private def cacheKey(req: InfoRequest): String = {
    val path = Paths.get(req.jobPath)
    val sha1 = DigestUtils.sha1Hex(Files.newInputStream(path))
    s"${req.name}_${req.className}_$sha1"
  }

}

object JobInfoProviderActor {

  def props(jobInfoExtractor: JobInfoExtractor, ttl: FiniteDuration = 3600 seconds): Props =
    Props(new JobInfoProviderActor(jobInfoExtractor, ttl))

}

