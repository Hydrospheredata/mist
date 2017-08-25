package io.hydrosphere.mist.jobs

import java.io.File

import cats.implicits._

import scala.util.{Failure, Success, Try}
import io.hydrosphere.mist.jobs.jar.{JobClass, JobsLoader}
import org.apache.commons.io.FilenameUtils

sealed trait JobInfo {

  def validateAction(
    params: Map[String, Any],
    action: Action): Either[Throwable, this.type]
}

case object PyJobInfo extends JobInfo {

  def validateAction(
    params: Map[String, Any],
    action: Action): Either[Throwable, PyJobInfo.this.type] = Right(this)
}

case class JvmJobInfo(jobClass: JobClass) extends JobInfo {

  def validateAction(
    params: Map[String, Any],
    action: Action): Either[Throwable, JvmJobInfo.this.type] = {

    val inst = action match {
      case Action.Execute => jobClass.execute
      case Action.Serve => jobClass.serve
    }
    inst match {
      case None => Left(new IllegalStateException(s"Job without $action job instance"))
      case Some(exec) => exec.validateParams(params).map(_ => this)
    }
  }
}

object JobInfo {

  def load(name: String, file: File, className: String): Try[JobInfo] = {
    val fileName = file.getName
    val extension = FilenameUtils.getExtension(fileName)
    createJobInfo(file, extension, className)
  }

  private def createJobInfo(file: File, extension: String, className: String): Try[JobInfo] = extension match {
    case "py" =>
      Success(PyJobInfo)
    case "jar" =>
      val inst = JobsLoader.fromJar(file).loadJobClass(className)
      inst.map(i => JvmJobInfo(i))
    case e =>
      Failure(new IllegalArgumentException(s"Unknown file format $e for $file"))
  }

}


