package io.hydrosphere.mist.master.artifact

import java.io.File

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
