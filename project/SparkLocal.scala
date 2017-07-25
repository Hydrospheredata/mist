import sbt._

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
