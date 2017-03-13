package io.hydrosphere.mist.lib.spark1

import org.apache.spark.SparkContext

trait ContextSupport {
  private var _context: SparkContext = _
  protected def context: SparkContext = _context

  private[mist] def setup(sc: ContextWrapper) = _context = sc.context
  private[mist] def stopStreaming(): Unit = {}
}
