package io.hydrosphere.mist.master.logging

import java.nio.file._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import io.hydrosphere.mist.api.logging.MistLogging.LogEvent

import scala.collection.mutable
import scala.concurrent.Future

//TODO: security problem - check path!
class LogsStore(directory: String) {

  val files = new ConcurrentHashMap[Path, FileEntry]()

  def store(event: LogEvent): Unit = {
    val path = mkPath(event.from)
    val message = event.mkString + "\n"
    val f = path.toFile
    if (!f.exists())
      Files.createFile(path)

    Files.write(path, message.getBytes, StandardOpenOption.APPEND)
  }

//  def write(from: String, events: Seq[LogEvent]): Future[Long] = {
//    val path = mkPath(from)
//    files.
//  }

  def pathToLogs(entryId: String): String = mkPath(entryId).toString

  def mkPath(entryId: String): Path =
    Paths.get(directory, s"$entryId.log")
}

object LogsStore {

  def apply(path: String): LogsStore = {
    val p = Paths.get(path)
    val dir = p.toFile
    if (!dir.exists()) dir.mkdir()

    new LogsStore(path)
  }
}

class FileEntry(path: Path) {

  val size = new AtomicLong(path.toFile.length())

  def write(bytes: Array[Byte]): Long = synchronized {
    Files.write(path, bytes, StandardOpenOption.APPEND)
    size.getAndAdd(bytes.length)
  }

}