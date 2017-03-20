package io.hydrosphere.mist.lib.spark1

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

case class SetupConfiguration(
  context: SparkContext,
  streamingDuration: Duration,
  publisherConnectionString: String
)

