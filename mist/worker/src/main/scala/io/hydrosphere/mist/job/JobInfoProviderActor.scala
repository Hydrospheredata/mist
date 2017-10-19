package io.hydrosphere.mist.job

import java.io.File

import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData.{GetJobInfo, ValidateJobParameters}
import org.apache.commons.io.FilenameUtils

import scala.util.{Failure, Success}

class SimpleJobInfoProviderActor() extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetJobInfo(className, jobPath) =>
      val file = new File(jobPath)

      if (file.exists()) {
        val infoExtractor = selectInfoExtractor(file.getAbsolutePath)
        val result = infoExtractor.extractInfo(file, className)
        val message = result.map(_.info) match {
          case Success(info) =>
            info
          case Failure(ex) =>
            Status.Failure(ex)
        }
        sender() ! message
      } else sender() ! Status.Failure(new IllegalArgumentException(s"File should exists in path $jobPath"))


    case ValidateJobParameters(className, jobPath, action, params) =>
      val file = new File(jobPath)
      if (file.exists()) {
        val infoExtractor = selectInfoExtractor(file.getAbsolutePath)
        val instance = infoExtractor.extractInstance(file, className, action)
        val message = instance match {
          case Success(i) => i.validateParams(params) match {
              case Right(_) => Status.Success(())
              case Left(ex) => Status.Failure(ex)
            }
          case Failure(ex) => Status.Failure(ex)
        }
        sender() ! message

      } else sender() ! Status.Failure(new IllegalArgumentException(s"File should exists in path $jobPath"))

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

