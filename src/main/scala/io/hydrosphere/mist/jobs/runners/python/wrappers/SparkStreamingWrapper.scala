package io.hydrosphere.mist.jobs.runners.python.wrappers

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.lib.{ContextWrapper, StreamingSupport}
import org.apache.spark.streaming.api.java.JavaStreamingContext

private[mist] class SparkStreamingWrapper(contextWrapper: ContextWrapper) extends StreamingSupport {
  _sc = contextWrapper.context
  def setStreamingContext(ssc: JavaStreamingContext): Unit = {
    _ssc = ssc.ssc
  }

  def getDurationSeconds: Int = {
    (MistConfig.Contexts.streamingDuration(_sc.appName).milliseconds/1000).toInt
  }
}
