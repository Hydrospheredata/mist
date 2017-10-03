package io.hydrosphere.mist.worker.runners.python.wrappers

import io.hydrosphere.mist.utils.Collections

class DataWrapper {

  private var data: java.util.HashMap[String, Any] = _

  def set(in: java.util.HashMap[String, Any]): Unit =
    data = in

  def get: Map[String, Any] = Collections.asScalaRecursively(data)
  

}
