package mist.api

import mist.api.args.{ArgsInstances, WithArgsScala}
import mist.api.data.JsLikeData
import mist.api.encoding.Encoder

/**
  * Scala api - root class for jobs definition
  *
  * Example:
  * {{{
  *
  * import mist.api._
  * import mist.api.DefaultEncoders._
  * import org.apache.spark.SparkContext
  *
  * object MyJob extends MistJob[Array[Int]] {
  *   override def handle = {
  *     withArgs(arg[Int]("number").onSparkContext((i: Int, sc: SparkContext) => {
  *       sc.parallelize(1 to i).map(_ * 2).collect()
  *     })
  *   }
  * }
  * }}}
  */
abstract class MistFn[A](implicit enc: Encoder[A])
  extends ArgsInstances
  with Contexts
  with MistExtrasDef
  with WithArgsScala {

  def handle: Handle[A]

  final def execute(ctx: FnContext): JobResult[JsLikeData] = {
    handle.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }

}

