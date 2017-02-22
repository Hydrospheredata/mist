package io.hydrosphere.mist.lib

import io.hydrosphere.mist.MistConfig
import io.hydrosphere.mist.contexts.ContextWrapper
import org.apache.spark.SparkContext
import org.apache.spark.streaming._

trait StreamingSupport extends ContextSupport {

  private[mist] var _sc: SparkContext = _
  private[mist] var _ssc: StreamingContext = _

  def createStreamingContext: StreamingContext = {
    _ssc = new StreamingContext(_sc, MistConfig().Contexts.streamingDuration(_sc.appName))
    _ssc
  }

  override  private[mist] def setup(sc: ContextWrapper): Unit = {
    super.setup(sc)
    _sc = sc.context
  }

  override private[mist] def stopStreaming(): Unit = {
    super.stopStreaming()
    _ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

}
