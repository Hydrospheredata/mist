package io.hydrosphere.mist.master.logging

import java.nio.file._

/**
  * Mapping to job logs files inside directory
  * File id should contains only accepted symbols ([a-zA-Z0-9_\\-]) by security reasons
  */
class LogStorageMappings(dir: Path) {

  val acceptedSymbols = "[a-zA-Z0-9_\\-]".r

  def pathFor(id: String): Path = {
    if (accept(id))
      dir.resolve(fileName(id))
    else
      throw new RuntimeException(s"Entry $id contains unacceptable symbols")
  }

  def fileName(id: String): String = s"job-$id.log"

  def accept(id: String): Boolean = {
    val length = id.length
    acceptedSymbols.findAllIn(id).toList.size == length
  }
}



object LogStorageMappings {

  /**
    * Logs inside directory
    * Directory will be created if it doesn't exists
    */
  def create(path: String): LogStorageMappings = {
    val p = Paths.get(path)
    val dir = p.toFile
    if (!dir.exists()) dir.mkdir()
    else if (dir.isFile) throw new IllegalArgumentException(s"Path $path already exists as file")

    new LogStorageMappings(p)
  }
}

