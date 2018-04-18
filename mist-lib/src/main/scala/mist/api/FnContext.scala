package mist.api

import mist.api.data.JsLikeMap
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

sealed trait FnContext{
  val params: JsLikeMap
}

case class FullFnContext(
  sc: SparkContext,
  streamingDuration: Duration,
  info: RuntimeJobInfo,
  params: JsLikeMap
) extends FnContext

object FnContext {

  def apply(userParams: JsLikeMap): FnContext = new FnContext {
    override val params: JsLikeMap = userParams
  }

  def apply(
    sc: SparkContext,
    params: JsLikeMap,
    streamingDuration: Duration = Duration(1000),
    info: RuntimeJobInfo = RuntimeJobInfo.Unknown): FullFnContext = FullFnContext(sc, streamingDuration, info, params)

}
