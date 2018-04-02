package mist.api

import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

sealed trait FnContext{
  val params: Map[String, Any]
}

case class FullFnContext(
  sc: SparkContext,
  streamingDuration: Duration,
  info: RuntimeJobInfo,
  params: Map[String, Any]
) extends FnContext

object FnContext {

  def apply(userParams: Map[String, Any]): FnContext = new FnContext {
    override val params: Map[String, Any] = userParams
  }

  def apply(
    sc: SparkContext,
    params: Map[String, Any],
    streamingDuration: Duration = Duration(1000),
    info: RuntimeJobInfo = RuntimeJobInfo.Unknown): FullFnContext = FullFnContext(sc, streamingDuration, info, params)

}
