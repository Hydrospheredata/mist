package io.hydrosphere.mist.master.logging

import java.nio.file.{Files, Path, Paths}

import io.hydrosphere.mist.api.LogEvent
import org.apache.log4j.Level

import scala.concurrent.Future

//TODO: security problem - check path!
class MistLogManager(dumpDirectory: String) {

  def store(event: LogEvent): Unit = {
    val path = mkPath(event.from)
    val message = event.mkString
    Files.write(path, message.getBytes)
  }

  def getById(id: String): Array[Byte] = {
    val path = mkPath(id)
    if (path.toFile.exists()) {
      Files.readAllBytes(mkPath(id))
    } else {
      Array.empty
    }
  }

  private def mkPath(entryId: String): Path =
    Paths.get(dumpDirectory, s"$entryId.log")
}
