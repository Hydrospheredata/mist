package org.apache.spark.streaming

import org.apache.spark.SparkContext

class MistStreamingContext(
  _sc: SparkContext,
  _batchDur: Duration
) extends StreamingContext(_sc, null, _batchDur) {

  override def stop(
    stopSparkContext: Boolean = false
  ): Unit = {
    super.stop(stopSparkContext = false, stopGracefully = false)
  }

  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
    super.stop(stopSparkContext, stopGracefully)
  }
}
