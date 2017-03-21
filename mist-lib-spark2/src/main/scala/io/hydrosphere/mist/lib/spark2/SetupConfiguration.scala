package io.hydrosphere.mist.lib.spark2

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

case class SetupConfiguration(
  context: SparkContext,
  streamingDuration: Duration,
  publisherConnectionString: String,
  publisherTopic: String
)

