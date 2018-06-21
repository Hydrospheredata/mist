package mist.api

import mist.api.data.JsMap
import org.scalatest._
import org.scalatest.prop.TableDrivenPropertyChecks._

import mist.api.encoding.JsSyntax._
import mist.api.encoding.defaults._

case class Z(hz: String, yoyo: Int)
case class ComplexCase(abc: Int, hehe: String, x: Boolean, z: Seq[Z])
case class Test(a: String, b: Int, z: Boolean, l: Long)

case class ScalingFactor(dmaId: Int, scalingFactor: Double)
case class StoreItem(id: Int, lat: Double, lon: Double)
case class VisitRateArguments(
  locationList: List[StoreItem],
  scalingFactors: Seq[ScalingFactor],
  campaignId: Int,
  dateStart: String,
  dateEnd: String,
  distToVisit: Double,
  proximityZone: Double,
  eventsFileLocation: String
)

case class NumArg(n: Int, mult: Int)
case class ArgsX(
  number: NumArg
)

class UserArgsSpec extends FunSpec with Matchers {

  import ArgsInstances._

  describe("basic args") {

    it("named arg") {
      val myArg = arg[Int]("a")

      myArg.extract(FnContext.onlyInput(JsMap("a" -> 5.js))) shouldBe Extracted(5)
      myArg.extract(FnContext.onlyInput(JsMap.empty)).isFailed shouldBe true
    }

    def failed: Failed = Failed.InternalError("missing value")
    val expected = Table[ArgDef[_], JsMap, Extraction[_]](
      ("arg", "data", "expected"),
      (arg[Boolean]("b"),        JsMap("b" -> true.js),  Extracted(true)),
      (arg[Boolean]("b", false), JsMap.empty,            Extracted(false)),
      (arg[Boolean]("b", true),  JsMap("b" -> false.js), Extracted(false)),
      (arg[Boolean]("c", true),  JsMap("c" -> "2".js),   failed),
      (arg[Boolean]("b"),        JsMap.empty,            failed),
      (arg[Option[Boolean]]("b"),JsMap("b" -> true.js),  Extracted(Some(true))),
      (arg[Option[Boolean]]("b"),JsMap.empty,            Extracted(None)),

      (arg[Int]("n"),         JsMap("n" -> 2.js),  Extracted(2)),
      (arg[Int]("n", 0),      JsMap.empty,         Extracted(0)),
      (arg[Int]("n"),         JsMap.empty,         failed),
      (arg[Option[Int]]("n"), JsMap("n" -> 42.js), Extracted(Some(42))),
      (arg[Option[Int]]("n"), JsMap.empty,         Extracted(None)),

      (arg[Long]("n"),         JsMap("n" -> 2.js),  Extracted(2L)),
      (arg[Long]("n"),         JsMap("n" -> 2L.js),  Extracted(2L)),

      (arg[String]("s"),          JsMap("s" -> "value".js), Extracted("value")),
      (arg[String]("s", "value"), JsMap.empty,           Extracted("value")),
      (arg[String]("s"),          JsMap.empty,           failed),
      (arg[Option[String]]("s"),  JsMap("s" -> "yoyo".js),  Extracted(Some("yoyo"))),
      (arg[Option[String]]("s"),  JsMap.empty,           Extracted(None)),

      (arg[Double]("d"),         JsMap("d" -> 2.4.js),  Extracted(2.4)),
      (arg[Double]("d", 2.2),    JsMap.empty,        Extracted(2.2)),
      (arg[Double]("d"),         JsMap.empty,        failed),
      (arg[Double]("d"),         JsMap("d" -> 2.js),    Extracted(2.0)),
      (arg[Option[Double]]("d"), JsMap("d" -> 42.1.js), Extracted(Some(42.1))),
      (arg[Option[Double]]("d"), JsMap.empty,        Extracted(None)),

      (arg[Seq[Int]]("ints"),       JsMap("ints" -> Seq(1,2,3).js), Extracted(Seq(1, 2, 3))),

      (arg[Seq[Double]]("doubles"), JsMap("doubles" -> Seq(1.1,2.2,3.3).js), Extracted(Seq(1.1, 2.2, 3.3))),

      (arg[Seq[String]]("strings"), JsMap("strings" -> Seq("a", "b", "c").js), Extracted(Seq("a", "b", "c"))),

      (arg[Seq[Boolean]]("booleans"), JsMap("booleans" -> Seq(true, false).js), Extracted(Seq(true, false)))
    )

    it("should extract expected result") {
      forAll(expected) { (arg, params, expected) =>
        val ctx = FnContext.onlyInput(params)
        val result = arg.extract(ctx)
        (expected, result) match {
          case (extr: Extracted[_], res: Extracted[_]) => res shouldBe extr
          case (extr: Failed, res: Extracted[_]) => fail(s"for $arg got $res, expected $extr")
          case (extr: Extracted[_], res: Failed) => fail(s"for $arg got $res, expected $extr")
          case _ =>
        }
      }
    }

  }

  describe("ArgDef - validate") {

    it("should fail validation on invalid params") {
      val argTest = arg[Int]("test")
      val res = argTest.validate(JsMap("missing" -> 42.js))
      res.isFailed shouldBe true
    }

    it("should success validation on valid params") {
      val argTest = arg[Int]("test")
      val res = argTest.validate(JsMap("test" -> 42.js))
      res shouldBe Extracted(())
    }

    it("should skip system arg definition") {
      val res = allArgs.validate(JsMap.empty)
      res.isExtracted shouldBe true
    }

    it("should validate .validated rules") {
      val argTest = arg[Int]("test").validated(n => n > 41)
      val res = argTest.validate(JsMap("test" -> 40.js))
      res.isFailed shouldBe true
    }
    it("should pass validation on .validates rules") {
      val argTest = arg[Int]("test").validated(n => n > 41)
      val res = argTest.validate(JsMap("test" -> 42.js))
      res.isExtracted shouldBe true
    }
    it("should validate after arg combine") {
      val argTest = arg[Int]("test") & arg[Int]("test2")
      argTest.validate(JsMap("test" -> 42.js, "test2" -> 40.js)).isExtracted shouldBe true
      argTest.validate(JsMap("test" -> 42.js, "missing" -> 0.js)).isFailed shouldBe true
      argTest.validate(JsMap("missing" -> 0.js, "test2" -> 40.js)).isFailed shouldBe true
      argTest.validate(JsMap.empty).isFailed shouldBe true
    }
    it("should validate .validated rules after arg combine") {
      val argTest = arg[Int]("test").validated(n => n > 40) & arg[Int]("test2")
      argTest.validate(JsMap("test"-> 39.js, "test2" -> 42.js)).isFailed shouldBe true
    }
  }

}
