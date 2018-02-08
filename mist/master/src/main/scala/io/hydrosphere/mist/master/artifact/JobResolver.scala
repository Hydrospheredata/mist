package io.hydrosphere.mist.master.artifact

import java.io.File

/**
  * Trait for various job path definition
  *
  * @see [[LocalResolver]]
  */
trait JobResolver {

  def exists: Boolean

  /**
    * @return local file path
    */
  def resolve(): File

}

class LocalResolver(path: String) extends JobResolver {

  override def exists: Boolean = {
    val file = new File(path)
    file.exists() && !file.isDirectory
  }

  override def resolve(): File = {
    if (!exists) {
      throw new RuntimeException(s"file $path not found")
    }

    new File(path)
  }
}

object JobResolver {

  val DefaultSavePath = "/tmp"

  def apply(jobPath: String): JobResolver = new LocalResolver(jobPath)
}
