package mist.api.data

import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

class JsLikeDataSpec extends FunSpec with Matchers {

  val rawToData = Table(
    ("raw", "data"),
    (1, JsLikeInt(1)),
    ("str", JsLikeString("str")),
    (1.2, JsLikeDouble(1.2)),
    (List(1, 2), JsLikeList(Seq(JsLikeInt(1), JsLikeInt(2)))),
    (Array(1, 2), JsLikeList(Seq(JsLikeInt(1), JsLikeInt(2)))),
    (Map("key" -> "value"), JsLikeMap(Map("key" -> JsLikeString("value"))))
  )

  it("should parse raw any structure") {
    forAll(rawToData) { (raw: Any, jsLike: JsLikeData) =>
      JsLikeData.fromAny(raw) shouldBe jsLike
    }
  }

 }
