package io.hydrosphere.mist.job

import java.io.File

import akka.actor._
import akka.pattern._

import io.hydrosphere.mist.core.CommonData.GetJobInfo
import io.hydrosphere.mist.core.jvmjob.{FullJobInfo, JobsLoader}
import io.hydrosphere.mist.worker.runners.ArtifactDownloader
import mist.api.UserInputArgument
import mist.api.internal.{JavaJobInstance, JobInstance, ScalaJobInstance}
import org.apache.spark.util.SparkClassLoader

import scala.util.{Failure, Success}

class CachingJobInfoProviderActor(artifactDownloader: ArtifactDownloader) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = cached(Map.empty)

  def cached(cache: Map[String, FullJobInfo]): Receive = {
    case GetJobInfo(className, jobPath, action) =>
      val f = for {
        file <- artifactDownloader.downloadArtifact(jobPath)
        jobInfo = {
          val loader = prepareClassloader(file)
          val jobsLoader = new JobsLoader(loader)
          jobsLoader.loadJobInstance(className, action) match {
            case Success(i) => i
            case Failure(ex) => throw ex
          }
        }
      } yield {
        val lang = jobInfo match {
          case _: ScalaJobInstance => "scala"
          case _: JavaJobInstance => "java"
          case _ => "python"
        }
        FullJobInfo(
          lang = lang,
          execute = jobInfo.describe().collect{case x: UserInputArgument => x},
          path = jobPath,
          className = className
        )
      }
      f.onComplete {
        case Success(fullJobInfo) =>
          context become cached(cache + (Seq(className, jobPath, action.toString).mkString("_")-> fullJobInfo))
      }
      f.pipeTo(sender())
  }

  private def prepareClassloader(file: File): ClassLoader = {
    val existing = this.getClass.getClassLoader
    val url = file.toURI.toURL
    val patched = SparkClassLoader.withURLs(existing, url)
    Thread.currentThread().setContextClassLoader(patched)
    patched
  }
}

object JobInfoProviderActor {

  def props(artifactDownloader: ArtifactDownloader): Props =
    Props(new CachingJobInfoProviderActor(artifactDownloader))

}

