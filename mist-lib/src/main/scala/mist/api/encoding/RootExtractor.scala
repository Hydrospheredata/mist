package mist.api.encoding

import mist.api.{Extraction, Failed, MObj, RootArgType}
import mist.api.data.{JsData, JsLikeMap}

trait RootExtractor[A] extends JsExtractor[A] {
  def `type`: MObj
  def apply(js: JsData): Extraction[A]
}

object RootExtractor {

  def apply[A](argType: MObj)(f: JsLikeMap => Extraction[A]): RootExtractor[A] = new RootExtractor[A] {
    def apply(js: JsData): Extraction[A] = js match {
      case m: JsLikeMap => f(m)
      case other =>
        Failed.InvalidType(argType.toString, other.toString)
    }
    val `type`: MObj = argType
  }
}

