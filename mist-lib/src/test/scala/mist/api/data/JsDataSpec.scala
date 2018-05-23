package mist.api.data

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.util
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._

import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

class JsDataSpec extends FunSpec with Matchers {
  import java.{lang => jl, util => ju}
  val rawToData = Table(
    ("raw", "data"),
    (1, JsNumber(1)),
    ("str", JsString("str")),
    (1.2, JsNumber(1.2)),
    (List(1, 2), JsList(Seq(JsNumber(1), JsNumber(2)))),
    (Array(1, 2), JsList(Seq(JsNumber(1), JsNumber(2)))),
    (Map("key" -> "value"), JsMap(Map("key" -> JsString("value"))))
  )

  val javaMap: ju.Map[String, jl.Integer] = {
    val m = new ju.HashMap[String, jl.Integer](1)
    m.put("test", new jl.Integer(42))
    m
  }

  val javaRawToData = Table(
    ("raw", "data"),
    (new jl.Integer(42), JsNumber(42)),
    (new jl.Double(42.0), JsNumber(42.0)),
    (ju.Arrays.asList(new jl.Integer(42)), JsList(Seq(JsNumber(42)))),
    (javaMap, JsMap(Map("test"-> JsNumber(42))))
  )


  it("should parse raw any structure") {
    forAll(rawToData) { (raw: Any, jsLike: JsData) =>
      JsData.fromScala(raw) shouldBe jsLike
    }
  }
  it("should parse raw any java structure") {
    forAll(javaRawToData){ (raw: Any, jsLike: JsData) =>
      JsData.fromJava(raw) shouldBe jsLike
    }
  }

  describe("JsLikeMap") {

    // problem with MapLike - akka can't serialize it
    // scala.collection.immutable.MapLike$$anon$2
    //    java.io.NotSerializableException: scala.collection.immutable.MapLike$$anon$2
    it("JsLikeMap should be serializable") {
      val map = Map("1" -> 1, "2" -> 2).mapValues(i => JsNumber(i))
      val jslikeMap = JsMap(map)

      val bos = new ByteArrayOutputStream
      val out = new ObjectOutputStream(bos)
      out.writeObject(jslikeMap)
      out.close()
    }
  }

  it("should return untyped map") {
    val js = JsMap(
      "a" -> 1.js,
      "b" -> false.js,
      "c" -> JsList(Seq(
        JsMap("x" -> "y".js)
      ))
    )
    val exp = Map(
      "a" -> 1,
      "b" -> false,
      "c" -> Seq(
        Map("x" -> "y")
      )
    )
    JsData.untyped(js) shouldBe exp
  }

}
