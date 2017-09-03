package io.hydrosphere.mist.master

import java.nio.file.{Files, Path, Paths}

import org.apache.commons.io.FileUtils

package object data {

  def checkDirectory(path: String): Path = {
    val p = Paths.get(path)
    val dir = p.toFile

    if (!dir.exists())
      Files.createDirectories(p)
    else if (dir.isFile)
      throw new IllegalArgumentException(s"Path $path already exists as file")

    p
  }
}
