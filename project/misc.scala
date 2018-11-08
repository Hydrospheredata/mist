import java.io.FileInputStream
import java.util.jar.{JarFile, JarInputStream}

import scala.annotation.tailrec
import sbt._

import scala.concurrent.Await

object Tar {

  import java.io._

  import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream}
  import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

  def extractTarGz(from: File, to: File): Unit = {
    val gzipIn = new GzipCompressorInputStream(new FileInputStream(from))
    val tarIn = new TarArchiveInputStream(gzipIn)
    extractTar(tarIn, to)
    gzipIn.close
    tarIn.close
  }


  @tailrec
  def extractTar(stream: TarArchiveInputStream, root: File): Unit = {

    def isExecutable(mode: Int): Boolean = (mode.toOctalString.head.toInt & 0x001) == 1

    def write(entry: TarArchiveEntry) = {
      val file = root / entry.getName
      if (entry.isDirectory) {
        IO.createDirectory(file)
      } else {
        val out = new FileOutputStream(file)
        IO.transfer(stream, out)
        out.close
        if (isExecutable(entry.getMode)) file.setExecutable(true)
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

  def downloadSpark(sparkVersion: String, scalaBinVersion: String, to: File): Unit = {
    val link = downloadUrl(sparkVersion, scalaBinVersion)
    val target = to / distrTar(sparkVersion, scalaBinVersion)
    Downloader.download(link, target)

    Tar.extractTarGz(target, to)
  }

  def downloadUrl(sparkV: String, scalaBinV: String): String = {
    val url = if (sparkV == "2.4.0") {
      "http://repo.hydrosphere.io/hydrosphere/static/spark"
    } else {
      s"https://archive.apache.org/dist/spark/spark-$sparkV"
    }
    val x = s"$url/${distrTar(sparkV, scalaBinV)}"
    println(x)
    x
  }

  def distrName(sparkV: String, scalaBinV: String): String = {
    val hadoopVersion = "2.7"
    val scalaPostfix = if (scalaBinV == "2.12") "-scala-2.12" else ""
    s"spark-$sparkV-bin-hadoop$hadoopVersion" + scalaPostfix
  }

  def distrTar(sparkV: String, scalaBinV: String): String = s"${distrName(sparkV, scalaBinV)}.tgz"

}

/**
  * Dummy stdout logger for process running
  */
object StdOutLogger extends scala.sys.process.ProcessLogger {

  override def err(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def out(s: => String): Unit =
    ConsoleOut.systemOut.println(s)

  override def buffer[T](f: => T): T = f

}

object Downloader {
  import scala.concurrent.ExecutionContext.Implicits._
  import scala.concurrent.duration._
  import gigahorse._
  import support.okhttp.Gigahorse

  def download(url: String, out: File): File = {
    val http = sbt.librarymanagement.Http.http
    val req = Gigahorse.url(url)
    Await.result(http.download(req, out), Duration.Inf)
  }
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
