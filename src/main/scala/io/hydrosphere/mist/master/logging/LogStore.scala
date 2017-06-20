package io.hydrosphere.mist.master.logging

import java.nio.file.{Files, Path, Paths, StandardOpenOption}

import io.hydrosphere.mist.api.logging.MistLogging.LogEvent

//TODO: security problem - check path!
class LogStore(dumpDirectory: String) {

  def store(event: LogEvent): Unit = {
    val path = mkPath(event.from)
    val message = event.mkString + "\n"
    val f = path.toFile
    if (!f.exists())
      Files.createFile(path)

    Files.write(path, message.getBytes, StandardOpenOption.APPEND)
  }

  def getById(id: String): Array[Byte] = {
    val path = mkPath(id)
    if (path.toFile.exists()) {
      Files.readAllBytes(mkPath(id))
    } else {
      Array.empty
    }
  }

  def pathToLogs(entryId: String): String = mkPath(entryId).toString

  private def mkPath(entryId: String): Path =
    Paths.get(dumpDirectory, s"$entryId.log")
}
