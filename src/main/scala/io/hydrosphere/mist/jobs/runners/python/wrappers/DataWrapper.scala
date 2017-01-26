package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.utils.Collections

private[mist] class DataWrapper {
  private var data: java.util.HashMap[String, Any] = _

  def set(in: java.util.HashMap[String, Any]): Unit = {
    println(in.getClass.getCanonicalName)
    data = in
  }
  def get: Map[String, Any] = Collections.asScalaRecursively(data)
  

}
