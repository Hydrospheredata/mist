package io.hydrosphere.mist

import java.nio.file.{Files, Paths}

class TestCompiler(outputDir: String) {

  import scala.tools.nsc._

  val out = Paths.get(outputDir).toFile
  out.getParentFile.mkdirs()
  out.mkdir()

  def compile(source: String, name: String): Seq[String] = {
    val tmpSource = Paths.get(outputDir, s"$name.scala")
    Files.write(tmpSource, source.getBytes)


    val g = new Global(buildSettings())
    val r = new g.Run
    r.compile(List(tmpSource.toString))

    r.compiledFiles.toSeq
  }

  private def buildSettings() = {
    val settings = new Settings()
    val urls = urlses(getClass.getClassLoader)
    val paths = urls.map(_.getPath).toSeq
    paths.foreach(settings.bootclasspath.append)

    settings.outputDirs.setSingleOutput(outputDir)

    settings
  }

  private def urlses(cl: ClassLoader): Array[java.net.URL] = cl match {
    case null => Array()
    case u: java.net.URLClassLoader => u.getURLs ++ urlses(cl.getParent)
    case _ => urlses(cl.getParent)
  }
}

// naive packager - only for testing purposes
// not support package directories structures
object JarPackager {

  import java.io._
  import java.util.jar._

  def pack(dir: String, to: String): Unit = {
    val manifest = new java.util.jar.Manifest()
    manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
    val f = new File(dir)
    val out = Paths.get(to, s"${f.getName}.jar").toFile
    val target = new JarOutputStream(new FileOutputStream(out), manifest)
    f.listFiles().foreach(addFile(_, target))
    target.close()
  }

  private def addFile(source: File, target: JarOutputStream): Unit = {
    val entry = new JarEntry(source.getName)
    entry.setTime(source.lastModified())
    target.putNextEntry(entry)
    val in = new BufferedInputStream(new FileInputStream(source))
    val buffer = new Array[Byte](1024)

    var count = 0
    while ( {
      count = in.read(buffer); count
    } != -1) {
      target.write(buffer, 0, count)
    }
    target.closeEntry()
    in.close()
  }
}
