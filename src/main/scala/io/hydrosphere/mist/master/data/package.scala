package io.hydrosphere.mist.master

import java.nio.file.{Path, Paths}

package object data {

  def checkDirectory(path: String): Path = {
    val p = Paths.get(path)
    val dir = p.toFile

    if (!dir.exists())
      dir.mkdir()
    else if (dir.isFile)
      throw new IllegalArgumentException(s"Path $path already exists as file")

    p
  }
}
