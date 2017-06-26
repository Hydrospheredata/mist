package io.hydrosphere.mist.api

import org.apache.spark.SparkContext

trait ContextSupport {
  private [mist] var _context: SparkContext = _
  protected def context: SparkContext = _context

  private[mist] def setup(conf: SetupConfiguration) = _context = conf.context
  private[mist] def stop(): Unit = {}

}
