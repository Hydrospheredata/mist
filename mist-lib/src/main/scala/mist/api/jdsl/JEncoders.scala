package mist.api.jdsl

import mist.api.data._
import mist.api.encoding.{JsEncoder, defaultEncoders}

object JEncoders {

  import java.{lang => jl}
  import java.{util => ju}
  import scala.collection.JavaConverters._

  val empty: JsEncoder[Void] = JsEncoder(_ => JsNull)

  val shortEncoder: JsEncoder[jl.Short] = JsEncoder(s => JsNumber(s))
  val intEncoder: JsEncoder[jl.Integer] = JsEncoder(i => JsNumber(i))
  val longEncoder: JsEncoder[jl.Long] = JsEncoder(l => JsNumber(l))

  val floatEncoder: JsEncoder[jl.Double] = JsEncoder(f => JsNumber(f))
  val doubleEncoder: JsEncoder[jl.Double] = JsEncoder(d => JsNumber(d))

  val stringEncoder: JsEncoder[jl.String] = defaultEncoders.stringEnc

  def optionalEncoderOf[T](enc: JsEncoder[T]): JsEncoder[ju.Optional[T]] = JsEncoder(o => {
    if (o.isPresent) enc(o.get()) else JsNull
  })

  def listEncoderOf[T](enc: JsEncoder[T]): JsEncoder[ju.List[T]] = JsEncoder(l => {
    val elems = l.asScala.map(e => enc(e))
    JsList(elems)
  })

  def mapEncoderOf[T](enc: JsEncoder[T]): JsEncoder[ju.Map[String, T]] = JsEncoder(m => {
    val elems = m.asScala.toSeq.map({case (k, v) => k -> enc(v)})
    JsMap(elems: _*)
  })

  val jsData: JsEncoder[JsData] = defaultEncoders.identity
}

