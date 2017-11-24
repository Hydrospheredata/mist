package mist.api.jdsl

import java.util

import mist.api.{ArgExtraction, Extracted, JobContext, Missing}
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
    (intArg("n"),    Seq("n" -> 2),  Extracted(2)),
    (intArg("n", 0), Seq.empty,      Extracted(0)),
    (intArg("n"),    Seq.empty,      miss),
    (optIntArg("n"),    Seq("n" -> 42), Extracted(java.util.Optional.of(42))),
    (optIntArg("n"),    Seq.empty,      Extracted(java.util.Optional.empty())),


    (stringArg("s"),          Seq("s" -> "value"),  Extracted("value")),
    (stringArg("s", "value"), Seq.empty,            Extracted("value")),
    (stringArg("s"),          Seq.empty,            miss),
    (optStringArg("s"),          Seq("s" -> "yoyo"),   Extracted(java.util.Optional.of("yoyo"))),
    (optStringArg("s"),          Seq.empty,            Extracted(java.util.Optional.empty())),

    (doubleArg("d"),      Seq("d" -> 2.4),  Extracted(2.4)),
    (doubleArg("d", 2.2), Seq.empty,        Extracted(2.2)),
    (doubleArg("d"),      Seq.empty,        miss),
    (optDoubleArg("d"),      Seq("d" -> 42.1), Extracted(java.util.Optional.of(42.1))),
    (optDoubleArg("d"),      Seq.empty,        Extracted(java.util.Optional.empty())),

    (intListArg("ints"),       Seq("ints" -> Seq(1,2,3)), Extracted(javaList(1, 2, 3))),

    (doubleListArg("doubles"), Seq("doubles" -> Seq(1.1,2.2,3.3)), Extracted(javaList(1.1, 2.2, 3.3))),

    (stringListArg("strings"), Seq("strings" -> Seq("a", "b", "c")), Extracted(javaList("a", "b", "c")))
  )

  it("should extract expected result") {
    forAll(expected) { (arg, params, expected) =>
      val ctx = JobContext(params.toMap)
      val result = arg.asScala.extract(ctx)
      (expected, result) match {
        case (extr: Extracted[_], res: Extracted[_]) => res shouldBe extr
        case (extr: Missing[_], res: Extracted[_]) => fail(s"for $arg got $res, expected $extr")
        case _ =>
      }
    }
  }

  it("should validate") {
    val arg = intArg("a").validated(new Func1[java.lang.Integer, java.lang.Boolean] {
      override def apply(a1: java.lang.Integer): java.lang.Boolean = a1 > 2
    }).asScala

    arg.extract(JobContext(Map("a" -> 5))) shouldBe Extracted(5)
    arg.extract(JobContext(Map("a" -> 1))) shouldBe a[Missing[_]]
  }

  def testCtx(params: (String, Any)*): JobContext = {
    JobContext(params.toMap)
  }
}
