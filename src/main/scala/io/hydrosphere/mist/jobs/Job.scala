package io.hydrosphere.mist.jobs

import java.io.FileReader

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.JobStatus.JobStatus

import scala.reflect.io.File

/** Job state statuses */
private[mist] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

trait Job{

  final val id = java.util.UUID.randomUUID.toString

  val jobRunnerName: String = "..."

  protected var _status = JobStatus.Initialized

  /** Status getter
    *
    * @return [[JobStatus]]
    */
  def status: JobStatus = _status

  val configuration : JobConfiguration

  def run(): Either[Map[String, Any], String]

}

object Job{
  def apply(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper, JobRunnerName: String):Job = {

    val path = jobConfiguration.jarPath.getOrElse(jobConfiguration.pyPath.getOrElse(""))
    val file = try {
       new FileReader(path)
    } catch {
      case e: Throwable => Right(e.toString)
    }

    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => new JobJar(jobConfiguration, contextWrapper, JobRunnerName)
      case "py" => new JobPy(jobConfiguration, contextWrapper, JobRunnerName)
      case _ => throw new Exception(Constants.Errors.extensionError)
    }
  }
}
