package mist.api

import mist.api.data.JsData

import scala.util.Try

/**
  * Scala api - root class for jobs definition
  *
  * Example:
  * {{{
  *
  * import mist.api._
  * import org.apache.spark.SparkContext
  *
  * object MyJob extends MistFn {
  *   override def handle = {
  *     withArgs(arg[Int]("number").onSparkContext((i: Int, sc: SparkContext) => {
  *       sc.parallelize(1 to i).map(_ * 2).collect()
  *     })
  *   }
  * }
  * }}}
  */
abstract class MistFn extends FnEntryPoint {

  def handle: Handle

  final def execute(ctx: FnContext): Try[JsData] = handle.invoke(ctx)

}

