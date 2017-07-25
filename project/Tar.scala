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
