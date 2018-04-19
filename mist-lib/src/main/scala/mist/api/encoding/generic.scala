package mist.api.encoding
import mist.api.{Extraction, MObj, RootArgType}
import mist.api.data.JsData

object generic {

  def extractor[A](implicit ext: ObjectExtractor[A]): RootExtractor[A] = new RootExtractor[A] {
    override def apply(js: JsData): Extraction[A] = ext.apply(js)
    override def `type`: MObj = ext.`type`
  }

  def encoder[A](implicit enc: ObjectEncoder[A]): JsEncoder[A] = JsEncoder(enc.apply)
}
