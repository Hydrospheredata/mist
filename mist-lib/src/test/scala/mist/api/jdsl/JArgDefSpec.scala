package mist.api.jdsl

import java.util

import mist.api.args.{ArgExtraction, Extracted, Missing}
import mist.api.FnContext
import org.scalatest.{FunSpec, Matchers}
import org.scalatest.prop.TableDrivenPropertyChecks._

class JArgDefSpec extends FunSpec with Matchers {


  import JArgsDef._

  def miss: Missing[Nothing] = Missing("")
  def javaList[T](values: T*): java.util.List[T] = {
    val list = new util.ArrayList[T]()
    values.foreach(v => list.add(v))
    list
  }

  val expected = Table[JUserArg[_], Seq[(String, Any)], ArgExtraction[_]](
    ("arg", "data", "expected"),
    (booleanArg("b"),        Seq("b" -> true), Extracted(true)),
    (booleanArg("b", false), Seq.empty,        Extracted(false)),
    (booleanArg("b", false), Seq("b" -> true), Extracted(true)),
    (booleanArg("b"),        Seq.empty,        miss),
    (optBooleanArg("b"),     Seq("b" -> true), Extracted(java.util.Optional.of(true))),
    (optBooleanArg("b"),     Seq.empty,        Extracted(java.util.Optional.empty())),

    (intArg("n"),    Seq("n" -> 2),  Extracted(2)),
    (intArg("n", 0), Seq.empty,      Extracted(0)),
    (intArg("n"),    Seq.empty,      miss),
    (optIntArg("n"), Seq("n" -> 42), Extracted(java.util.Optional.of(42))),
    (optIntArg("n"), Seq.empty,      Extracted(java.util.Optional.empty())),

    (stringArg("s"),          Seq("s" -> "value"),  Extracted("value")),
    (stringArg("s", "value"), Seq.empty,            Extracted("value")),
    (stringArg("s"),          Seq.empty,            miss),
    (optStringArg("s"),       Seq("s" -> "yoyo"),   Extracted(java.util.Optional.of("yoyo"))),
    (optStringArg("s"),       Seq.empty,            Extracted(java.util.Optional.empty())),

    (doubleArg("d"),      Seq("d" -> 2.4),  Extracted(2.4)),
    (doubleArg("d", 2.2), Seq.empty,        Extracted(2.2)),
    (doubleArg("d"),      Seq.empty,        miss),
    (optDoubleArg("d"),   Seq("d" -> 42.1), Extracted(java.util.Optional.of(42.1))),
    (optDoubleArg("d"),   Seq.empty,        Extracted(java.util.Optional.empty())),

    (intListArg("ints"),       Seq("ints" -> Seq(1,2,3)), Extracted(javaList(1, 2, 3))),

    (doubleListArg("doubles"), Seq("doubles" -> Seq(1.1,2.2,3.3)), Extracted(javaList(1.1, 2.2, 3.3))),

    (stringListArg("strings"), Seq("strings" -> Seq("a", "b", "c")), Extracted(javaList("a", "b", "c"))),

    (booleanListArg("boolean"), Seq("boolean" -> Seq(true, false)), Extracted(javaList(true, false)))
  )

  it("should extract expected result") {
    forAll(expected) { (arg, params, expected) =>
      val ctx = FnContext(params.toMap)
      val result = arg.asScala.extract(ctx)
      (expected, result) match {
        case (extr: Extracted[_], res: Extracted[_]) => res shouldBe extr
        case (extr: Extracted[_], res: Missing[_]) => fail(s"for $arg got $res, expected $extr")
        case (extr: Missing[_], res: Extracted[_]) => fail(s"for $arg got $res, expected $extr")
        case (extr: Missing[_], res: Missing[_]) =>
//        case (a, b) => println(s"WTF? $a $b")
      }
    }
  }

  it("should validate") {
    val arg = intArg("a").validated(new Func1[java.lang.Integer, java.lang.Boolean] {
      override def apply(a1: java.lang.Integer): java.lang.Boolean = a1 > 2
    }).asScala

    arg.extract(FnContext(Map("a" -> 5))) shouldBe Extracted(5)
    arg.extract(FnContext(Map("a" -> 1))) shouldBe a[Missing[_]]
  }

  def testCtx(params: (String, Any)*): FnContext = {
    FnContext(params.toMap)
  }
}
