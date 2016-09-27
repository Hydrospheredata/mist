package io.hydrosphere.mist.jobs

import java.io.File
import java.net.URI

import io.hydrosphere.mist.jobs.JobFile.FileType.FileType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

trait JobFile {
  def exists: Boolean
  def fileType: FileType
  def file: File
}

object JobFile {

  object FileType extends Enumeration {
    type FileType = Value
    val Jar, Python = Value
  }

  def apply(path: String): JobFile = {
    if (path.startsWith("hdfs://")) {
      new HDFSJobFile(path)
    } else {
      new LocalJobFile(path)
    }
  }

}

class LocalJobFile(path: String) extends JobFile {
  override def exists: Boolean = {
    val file = new File(path)
    file.exists() && !file.isDirectory
  }

  // TODO: check real file type
  override def fileType: FileType = {
    if (!exists) {
      // TODO: self exceptions
      throw new Exception("file not found")
    }

    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => JobFile.FileType.Jar
      case "py" => JobFile.FileType.Python
      case _ => throw new Exception(s"Unknown file type in $path")
    }
  }

  override def file: File = {
    if (!exists) {
      // TODO: self exceptions
      throw new Exception("file not found")
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

  // TODO: check real file type
  override def fileType: FileType = {
    if (!exists) {
      // TODO: self exceptions
      throw new Exception("file not found")
    }

    path.split('.').drop(1).lastOption.getOrElse("") match {
      case "jar" => JobFile.FileType.Jar
      case "py" => JobFile.FileType.Python
      case _ => throw new Exception(s"Unknown file type in $path")
    }
  }

  override def file: File = {
    if (!exists) {
      // TODO: self exceptions
      throw new Exception("file not found")
    }
    val remotePath = new Path(path)
    val localPath = new Path(s"/tmp/${remotePath.getName}")
    // TODO: check if file already in local FS
    fileSystem.copyToLocalFile(false, remotePath, localPath, true)

    new File(localPath.toString)
  }
}
