package mist.api

import mist.api.ArgDef._
import org.scalatest.{FunSpec, Matchers}
import shapeless.HNil

class ArgDefSpec extends FunSpec with Matchers {

  import DefaultEncoders._

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

  def testCtx(params: (String, Any)*): JobContext = {
    JobContext(null, params.toMap)
  }
}
