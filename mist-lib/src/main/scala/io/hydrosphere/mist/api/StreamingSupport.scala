package io.hydrosphere.mist.api

import org.apache.spark.streaming._

trait StreamingSupport extends ContextSupport {

  private[mist] var _ssc: StreamingContext = _

  def streamingContext: StreamingContext = _ssc

  override private[mist] def setup(conf: SetupConfiguration): Unit = {
    super.setup(conf)
    _ssc = StreamingContext.getActiveOrCreate(() => {
      new StreamingContext(conf.context, conf.streamingDuration)
    })
  }

  override private[mist] def stop(): Unit = {
    super.stop()
    _ssc.stop(stopSparkContext = false, stopGracefully = true)
  }

}
