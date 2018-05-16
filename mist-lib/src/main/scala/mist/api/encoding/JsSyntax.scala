package mist.api.encoding

import mist.api.data.JsData

object JsSyntax {

  implicit class JsOps[A](a: A)(implicit enc: JsEncoder[A]) {
    def js: JsData = enc(a)
  }
}
