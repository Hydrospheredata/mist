package io.hydrosphere.mist.worker.runners.python.wrappers

import org.apache.spark.streaming.Duration

//TODO remove
class SparkStreamingWrapper(duration: Duration) {

  def getDurationSeconds: Int = (duration.milliseconds / 1000).toInt
}
