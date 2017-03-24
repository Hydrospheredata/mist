package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.api._
import org.apache.spark.streaming.api.java.JavaStreamingContext

private[mist] class SparkStreamingWrapper(config: SetupConfiguration) extends StreamingSupport {

  _context = config.context
  
  def setStreamingContext(ssc: JavaStreamingContext): Unit = {
    _ssc = ssc.ssc
  }

  def getDurationSeconds: Int = (config.streamingDuration.milliseconds / 1000).toInt
}
