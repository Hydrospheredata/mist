package mist.api.args

import mist.api.{FnContext, Handle, JobFailure, JobSuccess}
import org.scalatest.{FunSpec, Matchers}
import shadedshapeless._

class ArgDefSpec extends FunSpec with Matchers {

  import ArgDef._

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
      data shouldBe Extracted("first", "second", "third", 4)
    }

    it("should fail all") {
      val combined = const("1") combine missing[Int]("msg1") combine missing[Int]("msg2")
      val data = combined.extract(testCtx())
      data shouldBe Missing("msg1, msg2")
    }
  }

  describe("job def") {

    it("should describe job") {
      val job42: Handle[Int] = const(40) & const(2) apply { (a: Int, b: Int) => a + b }
      val res = job42.invoke(testCtx())
      res shouldBe JobSuccess(42)
    }

    it("should apply arguments in correct order") {
      val hello: Handle[String] = const("W") & const("o") & const("r") & const("l") & const("d") apply {
        (a: String, b: String, c: String, d: String, e: String) =>
          s"Hello $a$b$c$d$e"
      }
      hello.invoke(testCtx()) shouldBe JobSuccess("Hello World")
    }

    it("shouldn't work with missing args") {
      val invalid: Handle[String] = const("valid") & missing[Int]("msg") & const("last") apply {
        (a: String, b: Int, c: String) => "wtf?"
      }
      invalid.invoke(testCtx()) shouldBe a[JobFailure[_]]
    }

    it("should fail on error") {
      val broken: Handle[String] = const("a")({(a: String) => throw new RuntimeException("broken") })
      broken.invoke(testCtx()) shouldBe a[JobFailure[_]]
    }

    it("should work with one arg") {
      val jobDef: Handle[String] = const("single") { (a: String) => a }
      val res = jobDef.invoke(testCtx())
      res shouldBe JobSuccess("single")
    }
  }

  def testCtx(params: (String, Any)*): FnContext = {
    FnContext(params.toMap)
  }
}
