package io.hydrosphere.mist.master.artifact

import java.io.File

/**
  * Trait for various job path definition
  *
  * @see [[LocalResolver]]
  * @see [[HDFSResolver]]
  * @see [[MavenArtifactResolver]]
  */
trait JobResolver {

  def exists: Boolean

  /**
    * @return local file path
    */
  def resolve(): File

}

object JobResolver {

  val DefaultSavePath = "/tmp"

  def fromPath(jobPath: String, savePath: String = DefaultSavePath): JobResolver = {
    if (jobPath.startsWith("hdfs://"))
      new HDFSResolver(jobPath, savePath)
    else if(jobPath.startsWith("mvn://"))
      MavenArtifactResolver.fromPath(jobPath, savePath)
    else
      new LocalResolver(jobPath)
  }
}
