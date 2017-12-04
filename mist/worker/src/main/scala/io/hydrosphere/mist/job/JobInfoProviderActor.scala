package io.hydrosphere.mist.job

import java.io.File
import java.nio.file.{Files, Paths}

import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData._
import io.hydrosphere.mist.core.jvmjob.FullJobInfo
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
      val file = new File(r.jobPath)
      if (file.exists() && file.isFile) {
        val key = cacheKey(r)
        val infoE = cache.get(key) match {
          case Some(i) =>
            Right(i)
          case None => jobInfo(r)
        }

        infoE match {
          case Right(i) =>
            sender() ! i.info
            context become cached(cache + (key -> i))
          case Left(ex) =>
            sender() ! Status.Failure(ex)
        }
      } else sender() ! Status.Failure(new IllegalArgumentException(s"File should exists in path ${r.jobPath}"))

    case req@ValidateJobParameters(_, _, action, params) =>
      def validJobInfo(info: FullJobInfo): Boolean = {
        action match {
          case Action.Execute => !info.isServe
          case Action.Serve   => info.isServe
        }
      }

      val file = new File(req.jobPath)
      if (file.exists() && file.isFile) {
        val key = cacheKey(req)
        val infoE = cache.get(key) match {
          case Some(jobInfo) if validJobInfo(jobInfo.info) => Right(jobInfo)
          case Some(jobInfo) =>
            val actionStored = if (jobInfo.info.isServe) "serve" else "execute"
            val message = s"Incorrect job info stored by action $action but required $actionStored"
            Left(new IllegalArgumentException(message))
          case None => jobInfo(req)
        }

        infoE match {
          case Right(jobInfo) =>
            val message = jobInfo.instance.validateParams(params) match {
              case Right(_) => Status.Success(())
              case Left(ex) => Status.Failure(ex)
            }
            sender() ! message
            context become cached(cache + (key -> jobInfo))

          case Left(ex) =>
            sender() ! Status.Failure(ex)
        }
      } else sender() ! Status.Failure(new IllegalArgumentException(s"File should exists in path ${req.jobPath}"))

    case GetCacheSize =>
      sender() ! cache.size

    case EvictCache =>
      context become cached(cache.evictAll)

  }

  private def jobInfo(req: JobInfoMessage): Either[Throwable, JobInfo] = {
    import req._
    jobInfoExtractor.extractInfo(new File(jobPath), className) match {
      case Success(info) =>
        Right(info)
      case Failure(ex) =>
        Left(ex)
    }
  }

  private def cacheKey(req: JobInfoMessage): String = {
    val path = Paths.get(req.jobPath)
    val sha1 = DigestUtils.sha1Hex(Files.newInputStream(path))
    s"${req.className}_$sha1"
  }

}

object JobInfoProviderActor {

  def props(jobInfoExtractor: JobInfoExtractor, ttl: FiniteDuration = 3600 seconds): Props =
    //TODO: replace hardcoded values
    Props(new JobInfoProviderActor(jobInfoExtractor, ttl))

}

