package mist.api

import mist.api.data.JsMap
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Duration

sealed trait FnContext{
  val params: JsMap
}

case class FullFnContext(
  sc: SparkContext,
  streamingDuration: Duration,
  info: RuntimeJobInfo,
  params: JsMap
) extends FnContext

object FnContext {

  def onlyInput(in: JsMap): FnContext = new FnContext {
    override val params: JsMap = in
  }

  def apply(
    sc: SparkContext,
    params: JsMap,
    streamingDuration: Duration = Duration(1000),
    info: RuntimeJobInfo = RuntimeJobInfo.Unknown): FullFnContext = FullFnContext(sc, streamingDuration, info, params)
}

/**
  * For java
  */
object FnContextBuilder {


  def create(sc: SparkContext, params: JsMap): FullFnContext = FnContext(sc, params)

  def create(
    sc: SparkContext,
    params: JsMap,
    streamingDuration: Duration): FullFnContext = FnContext(sc, params, streamingDuration)

  def create(
    sc: SparkContext,
    params: JsMap,
    streamingDuration: Duration,
    info: RuntimeJobInfo
  ): FullFnContext = FnContext(sc, params, streamingDuration, info)
}
