package mist.api

import mist.api.data.{JsData, JsMap}
import org.scalatest.{FunSpec, Matchers}

import scala.util._

class ArgDefSpec extends FunSpec with Matchers {

  import ArgsInstances._

  describe("const/missing") {

    it("should extract const") {
      const("1").extract(testCtx()) shouldBe Extracted("1")
    }

    it("should extract missing") {
      missing("msg").extract(testCtx()) shouldBe a[Failed]
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
      data shouldBe a[Failed]
    }
  }

  describe("job def") {

    it("should describe job") {
      val job42: RawHandle[Int] = const(40) & const(2) apply { (a: Int, b: Int) => a + b }
      val res = job42.invoke(testCtx())
      res shouldBe Success(42)
    }

    it("should apply arguments in correct order") {
      val hello: RawHandle[String] = const("W") & const("o") & const("r") & const("l") & const("d") apply {
        (a: String, b: String, c: String, d: String, e: String) =>
          s"Hello $a$b$c$d$e"
      }
      hello.invoke(testCtx()) shouldBe Success("Hello World")
    }

    it("shouldn't work with missing args") {
      val invalid: RawHandle[String] = const("valid") & missing[Int]("msg") & const("last") apply {
        (a: String, b: Int, c: String) => "wtf?"
      }
      invalid.invoke(testCtx()) shouldBe a[Failure[_]]
    }

    it("should fail on error") {
      val broken: RawHandle[String] = const("a")({(a: String) => throw new RuntimeException("broken") })
      broken.invoke(testCtx()) shouldBe a[Failure[_]]
    }

    it("should work with one arg") {
      val jobDef: RawHandle[String] = const("single") { (a: String) => a }
      val res = jobDef.invoke(testCtx())
      res shouldBe Success("single")
    }
  }

  def testCtx(params: (String, JsData)*): FnContext = {
    FnContext.onlyInput(JsMap(params:_*))
  }
}
