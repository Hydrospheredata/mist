import mist.api.all._
import mist.api.encoding.{JsEncoder, JsExtractor, ObjExt}
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

  implicit val reqExt: ObjExt[Req] = mist.api.encoding.generic.extractor.derive[Req]
  implicit val reqEnc: JsEncoder[Req] = mist.api.encoding.generic.encoder.derive[Req]
  implicit val respEnc: JsEncoder[Resp] = mist.api.encoding.generic.encoder.derive[Resp]

  override def handle: Handle = arg[Req].onSparkContext((req: Req, sc: SparkContext) => {
    val x = sc.parallelize(req.numbers).map(_ * req.mult).collect()
    Resp(x, req)
  })

}

