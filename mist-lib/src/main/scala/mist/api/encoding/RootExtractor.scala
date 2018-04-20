package mist.api.encoding

import mist.api.{Extraction, Failed, MObj, RootArgType}
import mist.api.data.{JsData, JsMap}

trait RootExtractor[A] extends JsExtractor[A] {
  def `type`: MObj
  def apply(js: JsData): Extraction[A]
}

object RootExtractor {

  def apply[A](argType: MObj)(f: JsMap => Extraction[A]): RootExtractor[A] = new RootExtractor[A] {
    def apply(js: JsData): Extraction[A] = js match {
      case m: JsMap => f(m)
      case other =>
        Failed.InvalidType(argType.toString, other.toString)
    }
    val `type`: MObj = argType
  }
}

