package io.hydrosphere.mist.jobs.resolvers

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

  //TODO: should be provided from config
  private val saveDirectory = "/tmp"

  def fromPath(path: String): JobResolver = {
    if (path.startsWith("hdfs://"))
      new HDFSResolver(path, saveDirectory)
    else if(path.startsWith("mvn://"))
      MavenArtifactResolver.fromPath(path)
    else
      new LocalResolver(path)
  }
}
