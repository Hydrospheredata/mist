package io.hydrosphere.mist.utils

import java.io.{File, FilenameFilter}

object fs {

  case class FileFilter(f: String => Boolean) extends FilenameFilter {
    override def accept(dir: File, name: String): Boolean = f(name)
  }

  def mkFilter(f: String => Boolean) = FileFilter(f)

}
