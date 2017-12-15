import java.io.FileInputStream
import java.util.jar.{JarFile, JarInputStream}

import scala.annotation.tailrec
import sbt._

object Tar {

  import java.io._

  import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
  import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

  val CanExecuteMode = 493

  def extractTarGz(from: File, to: File): Unit = {
    val gzipIn = new GzipCompressorInputStream(new FileInputStream(from))
    val tarIn = new TarArchiveInputStream(gzipIn)
    extractTar(tarIn, to)
    gzipIn.close
    tarIn.close
  }

  @tailrec
  def extractTar(stream: TarArchiveInputStream, root: File): Unit = {

    def write(entry: TarArchiveEntry) = {
      val file = root / entry.getName
      if (entry.isDirectory) {
        IO.createDirectory(file)
      } else {
        val out = new FileOutputStream(file)
        IO.transfer(stream, out)
        out.close
        if (entry.getMode == CanExecuteMode) file.setExecutable(true)
      }
    }

    Option(stream.getNextEntry).map(_.asInstanceOf[TarArchiveEntry]) match {
      case Some(entry) =>
        write(entry)
        extractTar(stream, root)
      case None =>
    }
  }

}

object SparkLocal {

  def downloadSpark(version: String, to: File): Unit = {
    val link = url(downloadUrl(version))
    val target = to / distrTar(version)

    IO.download(link, target)
    Tar.extractTarGz(target, to)
  }

  def downloadUrl(v: String): String =
    s"https://archive.apache.org/dist/spark/spark-$v/${distrTar(v)}"

  def distrName(v: String): String = {
    val hadoopVersion = if(v.startsWith("1.")) "2.6" else "2.7"
    s"spark-$v-bin-hadoop$hadoopVersion"
  }

  def distrTar(v: String): String = s"${distrName(v)}.tgz"

}

/**
  * Dummy stdout logger for process running
  */
object StdOutLogger extends ProcessLogger {

  override def error(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def info(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def buffer[T](f: => T): T = f

}

case class Semver(tuple: (Int, Int, Int))
  (implicit order: Ordering[(Int, Int, Int)]){

  def gteq(other: Semver): Boolean = order.gteq(tuple, other.tuple)

  override def toString = {
    import tuple._
    s"${_1}.${_2}.${_3}"
  }
}

object Semver {

  def apply(s: String): Semver = {
    val arr = s.split('.').map(_.toInt)
    val patched = Array(1, 0, 0).patch(0, arr, arr.length)
    Semver(patched(0), patched(1), patched(2))
  }

}
