package io.hydrosphere.mist.job

import akka.actor._
import akka.pattern._
import io.hydrosphere.mist.core.CommonData.{GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.worker.runners.ArtifactDownloader
import org.apache.commons.io.FilenameUtils
import io.hydrosphere.mist.utils.FutureOps._
import scala.concurrent.Future

class SimpleJobInfoProviderActor(artifactDownloader: ArtifactDownloader) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetJobInfo(className, jobPath) =>
      val result = artifactDownloader.downloadArtifact(jobPath).flatMap { file =>

        val infoExtractor = selectInfoExtractor(file.getAbsolutePath)
        val result = infoExtractor.extractInfo(file, className)
        Future.fromTry(result.map(_.info))
      }

      result pipeTo sender()

    case ValidateJobParameters(className, jobPath, action, params) =>
      val result = for {
        file          <- artifactDownloader.downloadArtifact(jobPath)
        infoExtractor =  selectInfoExtractor(file.getAbsolutePath)
        instance      <- Future.fromTry(infoExtractor.extractInstance(file, className, action))
        _             <- Future.fromEither(instance.validateParams(params))
      } yield ()
      result pipeTo sender()
  }

  private def selectInfoExtractor(filePath: String): JobInfoExtractor = {
    FilenameUtils.getExtension(filePath) match {
      case "py" => new PythonJobInfoExtractor
      case "jar" => new JvmJobInfoExtractor
    }
  }

}

object JobInfoProviderActor {

  def props(artifactDownloader: ArtifactDownloader): Props =
    Props(new SimpleJobInfoProviderActor(artifactDownloader))

}

