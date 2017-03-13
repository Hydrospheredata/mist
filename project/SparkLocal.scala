import sbt._

import scala.annotation.tailrec

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
}
