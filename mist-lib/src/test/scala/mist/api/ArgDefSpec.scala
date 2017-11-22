package mist.api

import mist.api.ArgDef._
import org.scalatest.{FunSpec, Matchers}
import shapeless.HNil

class ArgDefSpec extends FunSpec with Matchers {

  import JobDefInstances._

  describe("const/missing") {

    it("should extract const") {
      const("1").extract(testCtx()) shouldBe Extracted("1")
    }

    it("should extract missing") {
      missing("msg").extract(testCtx()) shouldBe Missing("msg")
    }
  }

  describe("combine") {

    it("should extract combined") {
      val combined = const("first") & const("second") & const("third") & const(4)

      val data = combined.extract(testCtx())
      data shouldBe Extracted("first" :: "second" :: "third" :: 4 :: HNil)
    }

    it("should fail all") {
      val combined = const("1") combine missing[Int]("msg1") combine missing[Int]("msg2")
      val data = combined.extract(testCtx())
      data shouldBe Missing("msg1, msg2")
    }
  }

  describe("job def") {

    it("should describe job") {
      val job42: JobDef[Int] = const(40) & const(2) apply { (a: Int, b: Int) => a + b }
      val res = job42.invoke(testCtx())
      res shouldBe JobSuccess(42)
    }

    it("should apply arguments in correct order") {
      val hello: JobDef[String] = const("W") & const("o") & const("r") & const("l") & const("d") apply {
        (a: String, b: String, c: String, d: String, e: String) =>
          s"Hello $a$b$c$d$e"
      }
      hello.invoke(testCtx()) shouldBe JobSuccess("Hello World")
    }

    it("shouldn't work with missing args") {
      val invalid: JobDef[String] = const("valid") & missing[Int]("msg") & const("last") apply {
        (a: String, b: Int, c: String) => "wtf?"
      }
      invalid.invoke(testCtx()) shouldBe a[JobFailure[_]]
    }

    it("should fail on error") {
      val broken: JobDef[Int] = const("a")({(a: String) => throw new RuntimeException("broken") })
      broken.invoke(testCtx()) shouldBe a[JobFailure[_]]
    }

    it("should work with one arg") {
      val jobDef: JobDef[String] = const("single") { (a: String) => a }
      val res = jobDef.invoke(testCtx())
      res shouldBe JobSuccess("single")
    }
  }


  describe("ArgDef combs") {

    it("for named arg") {
      val x = arg[Int]("abc")
      x.extract(testCtx("abc" -> 42)) shouldBe Extracted(42)
      x.extract(testCtx()) shouldBe a[Missing[_]]
      x.extract(testCtx("abc" -> "str")) shouldBe a[Missing[_]]
    }

    it("for opt named arg") {
      val x = optArg[Int]("opt")
      x.extract(testCtx("opt" -> 1)) shouldBe Extracted(Some(1))
      x.extract(testCtx()) shouldBe Extracted(None)
      x.extract(testCtx("opt" -> "str")) shouldBe a[Missing[_]]
    }

    it("for named arg with default value") {
      val x = arg[Int]("abc", -1)
      x.extract(testCtx("abc" -> 42)) shouldBe Extracted(42)
      x.extract(testCtx()) shouldBe Extracted(-1)
      x.extract(testCtx("abc" -> "str")) shouldBe a[Missing[_]]
    }

    it("all args") {
      allArgs.extract(testCtx("a" -> "b", "x" -> 42)) shouldBe
        Extracted(Map("a" -> "b", "x" -> 42))

      allArgs.extract(testCtx()) shouldBe Extracted(Map.empty)
    }
  }
  describe("ArgDef - validate") {

    it("should fail validation on invalid params") {
      val argTest = arg[Int]("test")
      val res = argTest.validate(Map("missing" -> 42))
      res.isLeft shouldBe true
    }

    it("should success validation on valid params") {
      val argTest = arg[Int]("test")
      val res = argTest.validate(Map("test" -> 42))
      res shouldBe Right(())
    }

    it("should skip system arg definition") {
      val res = allArgs.validate(Map.empty)
      res.isRight shouldBe true
    }

    it("should validate .validated rules") {
      val argTest = arg[Int]("test").validated(n => n > 41)
      val res = argTest.validate(Map("test" -> 40))
      res.isLeft shouldBe true
    }
    it("should pass validation on .validates rules") {
      val argTest = arg[Int]("test").validated(n => n > 41)
      val res = argTest.validate(Map("test" -> 42))
      res.isRight shouldBe true
    }
    it("should validate after arg combine") {
      val argTest = arg[Int]("test") & arg[Int]("test2")
      argTest.validate(Map("test" -> 42, "test2" -> 40)).isRight shouldBe true
      argTest.validate(Map("test" -> 42, "missing" -> 0)).isLeft shouldBe true
      argTest.validate(Map("missing" -> 0, "test2" -> 40)).isLeft shouldBe true
      argTest.validate(Map.empty).isLeft shouldBe true
    }
    it("should validate .validated rules after arg combine") {
      val argTest = arg[Int]("test").validated(n => n > 40) & arg[Int]("test2")
      argTest.validate(Map("test"-> 39, "test2" -> 42)).isLeft shouldBe true
    }
  }

  def testCtx(params: (String, Any)*): JobContext = {
    JobContext(params.toMap)
  }
}
