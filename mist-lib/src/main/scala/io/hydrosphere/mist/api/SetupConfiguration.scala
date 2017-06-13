package io.hydrosphere.mist.api

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

case class RuntimeJobInfo(
  id: String,
  workerId: String
)

case class CentralLoggingConf(
  host: String,
  port: Int
)

case class SetupConfiguration(
  context: SparkContext,
  streamingDuration: Duration,
  info: RuntimeJobInfo,
  loggingConf: Option[CentralLoggingConf]
)

