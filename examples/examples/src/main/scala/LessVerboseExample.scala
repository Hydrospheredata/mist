import mist.api._
import mist.api.dsl._
import mist.api.encoding._

import org.apache.spark.SparkContext

case class Req(
  numbers: Seq[Int],
  multiplier: Option[Int]
) {

  def mult: Int = multiplier.getOrElse(2)
}

case class Resp(
  wtf: Array[Int],
  req: Req
)

object LessVerboseExample extends MistFn {

  import defaults._

  implicit val reqExt: RootExtractor[Req] = encoding.generic.extractor[Req]
  implicit val reqEnc: JsEncoder[Req] = encoding.generic.encoder[Req]
  implicit val respEnc: JsEncoder[Resp] = encoding.generic.encoder[Resp]

  override def handle: Handle = arg[Req].onSparkContext((req: Req, sc: SparkContext) => {
    val x = sc.parallelize(req.numbers).map(_ * req.mult).collect()
    Resp(x, req)
  }).asHandle

}

