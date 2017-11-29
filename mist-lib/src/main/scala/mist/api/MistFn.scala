package mist.api

import mist.api.args.{ArgsInstances, WithArgsScala}
import mist.api.data.JsLikeData
import mist.api.encoding.Encoder

/**
  * Scala api - root class for jobs definition
  *
  * Example:
  * <pre>
  * {@code
  *
  * import mist.api._
  * import mist.api.DefaultEncoders._
  * import org.apache.spark.SparkContext
  *
  * object MyJob extends MistJob[Array[Int]] {
  *   override def defineJob = {
  *     withArgs(arg[Int]("number").onSparkContext((i: Int, sc: SparkContext) => {
  *       sc.parallelize(1 to i).map(_ * 2).collect()
  *     })
  *   }
  * }
  * }
  * </pre>
  */
abstract class MistFn[A](implicit enc: Encoder[A])
  extends ArgsInstances
  with Contexts
  with MistExtrasDef
  with WithArgsScala {

  def handler: FnDef[A]

  final def execute(ctx: FnContext): JobResult[JsLikeData] = {
    handler.invoke(ctx) match {
      case JobSuccess(data) => JobSuccess(enc(data))
      case f: JobFailure[_] => f.asInstanceOf[JobFailure[JsLikeData]]
    }
  }

}

