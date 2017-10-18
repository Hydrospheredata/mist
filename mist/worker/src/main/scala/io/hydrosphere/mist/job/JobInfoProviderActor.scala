package io.hydrosphere.mist.job

import java.io.File

import akka.actor.{Status, _}
import akka.pattern._
import io.hydrosphere.mist.core.CommonData.{GetJobInfo, ValidateJobParameters}
import io.hydrosphere.mist.worker.runners.ArtifactDownloader
import org.apache.commons.io.FilenameUtils
import io.hydrosphere.mist.utils.FutureOps._

import scala.concurrent.Future
import scala.util.{Failure, Success}

class SimpleJobInfoProviderActor() extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetJobInfo(className, jobPath) =>
      val file = new File(jobPath)
      var message = _
      if (file.exists()) {
        val infoExtractor = selectInfoExtractor(file.getAbsolutePath)
        val result = infoExtractor.extractInfo(file, className)
        result.map(_.info) match {
          case Success(info) =>
            message = info
          case Failure(ex) =>
            message = Status.Failure(ex)
        }
      } else message = Status.Failure(new IllegalArgumentException(s"File should exists in path $jobPath"))

      sender() ! message

    case ValidateJobParameters(className, jobPath, action, params) =>
      val file = new File(jobPath)
      var message = _
      if (file.exists()) {
        val infoExtractor = selectInfoExtractor(file.getAbsolutePath)
        val instance = infoExtractor.extractInstance(file, className, action)
        message = instance match {
          case Success(i) => i.validateParams(params) match {
              case Right(_) =>
              case Left(ex) => Status.Failure(ex)
            }
          case Failure(ex) => Status.Failure(ex)
        }
      } else message = Status.Failure(new IllegalArgumentException(s"File should exists in path $jobPath"))

      sender() ! message
  }

  private def selectInfoExtractor(filePath: String): JobInfoExtractor = {
    FilenameUtils.getExtension(filePath) match {
      case "py" => new PythonJobInfoExtractor
      case "jar" => new JvmJobInfoExtractor
    }
  }

}

object JobInfoProviderActor {

  def props(): Props =
    Props(new SimpleJobInfoProviderActor())

}

