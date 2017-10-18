package io.hydrosphere.mist.job

import java.io.File

import akka.actor._
import akka.pattern._
import io.hydrosphere.mist.core.CommonData.{Action, GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.core.jvmjob.{FullJobInfo, JobsLoader}
import io.hydrosphere.mist.worker.runners.ArtifactDownloader
import mist.api.UserInputArgument
import mist.api.internal.{BaseJobInstance, JavaJobInstance}
import org.apache.commons.io.FilenameUtils
import org.apache.spark.util.SparkClassLoader

import scala.util.Try

class SimpleJobInfoProviderActor(artifactDownloader: ArtifactDownloader) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetJobInfo(className, jobPath) =>
      val result = artifactDownloader.downloadArtifact(jobPath).map { file =>
        FilenameUtils.getExtension(file.getAbsolutePath) match {
          case "py" =>
            Some(FullJobInfo(
              lang = "python",
              className = className,
              path = jobPath
            ))
          case "jar" =>
            val executeJobInstance = loadJobInstance(className, file, Action.Execute).toOption
            val serveJobInstance = loadJobInstance(className, file, Action.Serve).toOption
            executeJobInstance orElse serveJobInstance map { info =>
              val lang = info match {
                case _: JavaJobInstance => "java"
                case _ => "scala"
              }
              FullJobInfo(
                lang = lang,
                execute = info.describe().collect { case x: UserInputArgument => x },
                path = jobPath,
                className = className
              )
            }
        }
      }

      result pipeTo sender()

    case ValidateJobParameters(className, jobPath, action, params) =>
      val result = artifactDownloader.downloadArtifact(jobPath).map { file =>
        loadJobInstance(className, file, action)
          .toOption
          .map(_.validateParams(params))
      }

      result pipeTo sender()
  }

  private def loadJobInstance(className: String, file: File, action: Action): Try[BaseJobInstance] = {
    val loader = prepareClassloader(file)
    val jobsLoader = new JobsLoader(loader)
    jobsLoader.loadJobInstance(className, action)
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
    Props(new SimpleJobInfoProviderActor(artifactDownloader))

}

