package io.hydrosphere.mist.jobs

import java.io.File
import java.net.URI

import io.hydrosphere.mist.jobs.JobFile.FileType.FileType
import io.hydrosphere.mist.utils.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait JobFile extends Logger {
  def exists: Boolean
  def file: File
}

object JobFile {

  class NotFoundException(message: String) extends Exception
  class UnknownTypeException(message: String) extends Exception

  object FileType extends Enumeration {
    type FileType = Value
    val Jar, Python = Value
  }

  def apply(path: String): JobFile = {
    if (path.startsWith("hdfs://")) {
      new HDFSJobFile(path)
    } else if(path.startsWith("mvn://")) {
      MavenArtifactResolver.fromPath(path)
    } else {
      new LocalJobFile(path)
    }
  }

  def fileType(path: String): FileType = path match {
    case p if p.startsWith("mvn://") || p.endsWith(".jar") =>
      JobFile.FileType.Jar
    case p if p.endsWith(".py") =>
      JobFile.FileType.Python
    case _ =>
      throw new UnknownTypeException(s"Unknown file type in $path")
  }

}

class LocalJobFile(path: String) extends JobFile {
  override def exists: Boolean = {
    val file = new File(path)
    file.exists() && !file.isDirectory
  }

  override def file: File = {
    if (!exists) {
      logger.error(s"file $path not found")
      throw new JobFile.NotFoundException(s"file $path not found")
    }

    new File(path)
  }
}

class HDFSJobFile(path: String) extends JobFile {

  private val uri = new URI(path)

  private val hdfsAddress = s"${uri.getScheme}://${uri.getHost}:${uri.getPort}"

  private lazy val fileSystem = {
    FileSystem.get(new URI(hdfsAddress), new Configuration())
  }

  override def exists: Boolean = {
    fileSystem.exists(new Path(uri.getPath))
  }

  override def file: File = {
    if (!exists) {
      throw new JobFile.NotFoundException(s"file $path not found")
    }
    val remotePath = new Path(path)
    val checkSum = fileSystem.getFileChecksum(remotePath)
    val localPath = new Path(s"/tmp/${checkSum.toString}")
    if (!new File(localPath.toString).exists()) {
      fileSystem.copyToLocalFile(false, remotePath, localPath, true)
    }

    new File(localPath.toString)
  }
}
