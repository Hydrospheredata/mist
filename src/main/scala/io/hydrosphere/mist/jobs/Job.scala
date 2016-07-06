package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.JobStatus.JobStatus

/** Job state statuses */
private[mist] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

trait Job{

  final val id = java.util.UUID.randomUUID.toString

  val jobRunnerName: String = "..."

  protected var _status = JobStatus.Initialized

  //def initSqlContext(): Unit = ???
  //def initHiveContext(): Unit = ???

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
    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => new JobJar(jobConfiguration, contextWrapper, JobRunnerName)
      case "py" => new JobPy(jobConfiguration, contextWrapper, JobRunnerName)
      case _ => throw new Exception(Constants.Errors.extensionError)
    }
  }
}
