package com.provectus.mist.jobs

//import com.provectus.mist.{Constants, mistJob}
import com.provectus.mist.Constants
import com.provectus.mist.contexts.ContextWrapper

/** Job state statuses */
private[mist] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

trait Job{

  final val id = java.util.UUID.randomUUID.toString

  protected var _status = JobStatus.Initialized

  def initSqlContext: Unit = ???
  def initHiveContext: Unit = ???

  /** Status getter
    *
    * @return [[JobStatus]]
    */
  def status = _status

  def run(): Either[Map[String, Any], String]
}

object Job{
  def apply(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper):Job = {
    val path = jobConfiguration.jarPath.getOrElse(jobConfiguration.pyPath.getOrElse(""))
    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => return new JobJar(jobConfiguration, contextWrapper)
      case "py" => return new JobPy(jobConfiguration, contextWrapper)
      case _ => throw new Exception(Constants.Errors.extensionError)
    }
  }
}