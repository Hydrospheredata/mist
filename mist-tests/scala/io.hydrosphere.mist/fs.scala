package io.hydrosphere.mist

import java.nio.file.{ Files, Path }

import scala.collection.JavaConversions._

// fs utils
object fs {

  def list(p: Path): List[Path] =
    p.toFile.list().map(p.resolve).toList

  def clean(path: Path): Unit = {
    val file = path.toFile
    if (file.isDirectory) {
      list(path).foreach(clean)
      file.delete()
    } else
      file.delete()
  }

  def createDir(path: Path): Unit = {
    Files.createDirectories(path)
  }

  def copyFile(from: Path, to: Path): Path = {
    val data = Files.readAllBytes(from)
    Files.write(to, data)
  }

  def copyDir(from: Path, to: Path): Unit = copyDir(from, to, _ => true)

  def copyDir(from: Path, to: Path, filter: Path => Boolean): Unit = {
    if (!to.toFile.exists()) createDir(to)
    list(from)
      .filter(filter)
      .foreach(p => {
        if (p.toFile.isFile)
          copyFile(p, to.resolve(p.last))
        else {
          val next = to.resolve(p.last)
          createDir(next)
          copyDir(p, next, filter)
        }
      })
  }

}