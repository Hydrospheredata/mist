package io.hydrosphere.mist.jobs.resolvers

import java.io.File

import io.hydrosphere.mist.jobs.JobFile

class LocalResolver(path: String) extends JobResolver {

  override def exists: Boolean = {
    val file = new File(path)
    file.exists() && !file.isDirectory
  }

  override def resolve(): File = {
    if (!exists) {
      throw new JobFile.NotFoundException(s"file $path not found")
    }

    new File(path)
  }
}
