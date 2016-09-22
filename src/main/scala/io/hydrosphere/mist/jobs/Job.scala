package io.hydrosphere.mist.jobs

import io.hydrosphere.mist.{Constants, Logger}
import io.hydrosphere.mist.contexts.ContextWrapper
import io.hydrosphere.mist.jobs.JobStatus.JobStatus
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import java.io.File
import java.net.URI

/** Job state statuses */
private[mist] object JobStatus extends Enumeration {
  type JobStatus = Value
  val Initialized, Running, Stopped, Aborted = Value
}

trait Job extends Logger{

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

object Job extends Logger{
  def apply(jobConfiguration: JobConfiguration, contextWrapper: ContextWrapper, JobRunnerName: String):Job = {

    val path = jobConfiguration.jarPath.getOrElse(jobConfiguration.pyPath.getOrElse(""))
    if (path.startsWith("hdfs://")) {
      val conf = new Configuration()
      val uriParts = path.replaceFirst("hdfs://", "").split("/", 2)
      val fileSystem = FileSystem.get(new URI(s"hdfs://${uriParts(0)}"), conf)
      if (!fileSystem.exists(new Path(s"/${uriParts(1)}"))) {
        throw new Exception(s"$path does not exist")
      }
    } else {
      val file = new File(path)
      if(!file.exists() || file.isDirectory) {
          throw new Exception(s"$path does not exist")
      }
    }

    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => new JobJar(jobConfiguration, contextWrapper, JobRunnerName)
      case "py" => new JobPy(jobConfiguration, contextWrapper, JobRunnerName)
      case _ => throw new Exception(Constants.Errors.extensionError)
    }
  }
}
