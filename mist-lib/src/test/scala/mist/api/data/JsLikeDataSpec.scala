package mist.api.data

import java.util

import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

class JsLikeDataSpec extends FunSpec with Matchers {
  import java.{lang => jl, util => ju}
  val rawToData = Table(
    ("raw", "data"),
    (1, JsLikeNumber(1)),
    ("str", JsLikeString("str")),
    (1.2, JsLikeNumber(1.2)),
    (List(1, 2), JsLikeList(Seq(JsLikeNumber(1), JsLikeNumber(2)))),
    (Array(1, 2), JsLikeList(Seq(JsLikeNumber(1), JsLikeNumber(2)))),
    (Map("key" -> "value"), JsLikeMap(Map("key" -> JsLikeString("value"))))
  )

  val javaMap: ju.Map[String, jl.Integer] = {
    val m = new ju.HashMap[String, jl.Integer](1)
    m.put("test", new jl.Integer(42))
    m
  }

  val javaRawToData = Table(
    ("raw", "data"),
    (new jl.Integer(42), JsLikeNumber(42)),
    (new jl.Double(42.0), JsLikeNumber(42.0)),
    (ju.Arrays.asList(new jl.Integer(42)), JsLikeList(Seq(JsLikeNumber(42)))),
    (javaMap, JsLikeMap(Map("test"-> JsLikeNumber(42))))
  )


  it("should parse raw any structure") {
    forAll(rawToData) { (raw: Any, jsLike: JsLikeData) =>
      JsLikeData.fromScala(raw) shouldBe jsLike
    }
  }
  it("should parse raw any java structure") {
    forAll(javaRawToData){ (raw: Any, jsLike: JsLikeData) =>
      JsLikeData.fromJava(raw) shouldBe jsLike
    }
  }

}
