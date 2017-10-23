package io.hydrosphere.mist.job

import java.io.File

import akka.actor.{Status, _}
import io.hydrosphere.mist.core.CommonData.{GetJobInfo, ValidateJobParameters}

import scala.util.{Failure, Success}

class SimpleJobInfoProviderActor(
  jobInfoExtractor: JobInfoExtractor
) extends Actor with ActorLogging {

  implicit val ec = context.dispatcher

  override def receive: Receive = {
    case GetJobInfo(className, jobPath) =>
      val file = new File(jobPath)

      if (file.exists() && file.isFile) {
        val result = jobInfoExtractor.extractInfo(file, className)
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
        val instance = jobInfoExtractor.extractInstance(file, className, action)
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

}

object JobInfoProviderActor {

  def props(jobInfoExtractor: JobInfoExtractor): Props =
    Props(new SimpleJobInfoProviderActor(jobInfoExtractor))

}

