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
  def resolve: File

}
