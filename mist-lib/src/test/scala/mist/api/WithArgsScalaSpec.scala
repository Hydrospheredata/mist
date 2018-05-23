package mist.api

import mist.api.data.JsMap
import mist.api.encoding.JsSyntax._
import org.scalatest.{FunSpec, Matchers}
import shadedshapeless.HNil

class WithArgsScalaSpec extends FunSpec with Matchers {

  import mist.api.ArgsInstances._
  import mist.api.encoding.defaults._
  import WithArgsScala._

  it("should apply tuples") {
    val result = withArgs(const("a"), const("b"), const("c"), const(5), const("last"))
    val extraction = result.extract(FnContext.onlyInput(JsMap.empty))
    extraction shouldBe Extracted("a", "b", "c", 5, "last")
  }

  it("should flat tuples") {
    val result = withArgs(const("a") & const("b"), const("c") & const(5) & const("last"))
    val extraction = result.extract(FnContext.onlyInput(JsMap.empty))
    extraction shouldBe Extracted("a", "b", "c", 5, "last")
  }

  it("should apply single element") {
    val result = withArgs(const("a"))
    val extraction = result.extract(FnContext.onlyInput(JsMap.empty))
    extraction shouldBe Extracted("a")
  }

  it("should work with user args") {
    import ArgsInstances._
    val result = withArgs((arg[Int]("n"): UserArg[Int], arg[Int]("m")))
    val extraction = result.extract(FnContext.onlyInput(JsMap("n" -> 5.js, "m" -> 10.js)))
    extraction shouldBe Extracted(5, 10)
  }

  it("should work with single user arg") {
    import ArgsInstances._
    val result = withArgs(arg[Int]("n"): UserArg[Int])
    val extraction = result.extract(FnContext.onlyInput(JsMap("n" -> 5.js, "m" -> 10.js)))
    extraction shouldBe Extracted(5)
  }

}
