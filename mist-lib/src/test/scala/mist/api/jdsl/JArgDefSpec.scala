package mist.api.jdsl

import java.util

import mist.api._
import mist.api.FnContext
import mist.api.data.JsMap
import mist.api.encoding.defaultEncoders._
import mist.api.encoding.JsSyntax._
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

class JArgDefSpec extends FunSpec with Matchers {


  import JArgsDef._

  def miss: Failed = Failed.InternalError("fail")
  def javaList[T](values: T*): java.util.List[T] = {
    val list = new util.ArrayList[T]()
    values.foreach(v => list.add(v))
    list
  }

  val expected = Table[JUserArg[_], JsMap, Extraction[_]](
    ("arg", "data", "expected"),
    (booleanArg("b"),        JsMap("b" -> true.js), Extracted(true)),
    (booleanArg("b", false), JsMap.empty,        Extracted(false)),
    (booleanArg("b", false), JsMap("b" -> true.js), Extracted(true)),
    (booleanArg("b"),        JsMap.empty,        miss),
    (optBooleanArg("b"),     JsMap("b" -> true.js), Extracted(java.util.Optional.of(true))),
    (optBooleanArg("b"),     JsMap.empty,        Extracted(java.util.Optional.empty())),

    (intArg("n"),    JsMap("n" -> 2.js),  Extracted(2)),
    (intArg("n", 0), JsMap.empty,      Extracted(0)),
    (intArg("n"),    JsMap.empty,      miss),
    (optIntArg("n"), JsMap("n" -> 42.js), Extracted(java.util.Optional.of(42))),
    (optIntArg("n"), JsMap.empty,      Extracted(java.util.Optional.empty())),

    (stringArg("s"),          JsMap("s" -> "value".js),  Extracted("value")),
    (stringArg("s", "value"), JsMap.empty,            Extracted("value")),
    (stringArg("s"),          JsMap.empty,            miss),
    (optStringArg("s"),       JsMap("s" -> "yoyo".js),   Extracted(java.util.Optional.of("yoyo"))),
    (optStringArg("s"),       JsMap.empty,            Extracted(java.util.Optional.empty())),

    (doubleArg("d"),      JsMap("d" -> 2.4.js),  Extracted(2.4)),
    (doubleArg("d", 2.2), JsMap.empty,        Extracted(2.2)),
    (doubleArg("d"),      JsMap.empty,        miss),
    (optDoubleArg("d"),   JsMap("d" -> 42.1.js), Extracted(java.util.Optional.of(42.1))),
    (optDoubleArg("d"),   JsMap.empty,        Extracted(java.util.Optional.empty())),

    (intListArg("ints"),       JsMap("ints" -> Seq(1,2,3).js), Extracted(javaList(1, 2, 3))),

    (doubleListArg("doubles"), JsMap("doubles" -> Seq(1.1,2.2,3.3).js), Extracted(javaList(1.1, 2.2, 3.3))),

    (stringListArg("strings"), JsMap("strings" -> Seq("a", "b", "c").js), Extracted(javaList("a", "b", "c"))),

    (booleanListArg("boolean"), JsMap("boolean" -> Seq(true, false).js), Extracted(javaList(true, false)))
  )

  it("should extract expected result") {
    forAll(expected) { (arg, params, expected) =>
      val ctx = FnContext.onlyInput(params)
      val result = arg.asScala.extract(ctx)
      (expected, result) match {
        case (extr: Extracted[_], res: Extracted[_]) => res shouldBe extr
        case (extr: Extracted[_], res: Failed) => fail(s"for $arg got $res, expected $extr")
        case (extr: Failed, res: Extracted[_]) => fail(s"for $arg got $res, expected $extr")
        case (extr: Failed, res: Failed) =>
      }
    }
  }

  it("should validate") {
    val arg = intArg("a").validated(new Func1[java.lang.Integer, java.lang.Boolean] {
      override def apply(a1: java.lang.Integer): java.lang.Boolean = a1 > 2
    }).asScala

    arg.extract(FnContext.onlyInput(JsMap("a" -> 5.js))) shouldBe Extracted(5)
    arg.extract(FnContext.onlyInput(JsMap("a" -> 1.js))) shouldBe a[Failed]
  }

}
