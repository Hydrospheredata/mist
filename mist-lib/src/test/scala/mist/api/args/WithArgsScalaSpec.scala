package mist.api.args

import mist.api.FnContext
import org.scalatest.{FunSpec, Matchers}
import shadedshapeless.HNil

class WithArgsScalaSpec extends FunSpec with Matchers {

  import ArgDef._
  import WithArgsScala._

  it("should apply tuples") {
    val result = withArgs(const("a"), const("b"), const("c"), const(5), const("last"))
    val extraction = result.extract(FnContext(Map.empty))
    extraction shouldBe Extracted("a" :: "b" :: "c" :: 5 :: "last" :: HNil)
  }

  it("should apply single element") {
    val result = withArgs(const("a"))
    val extraction = result.extract(FnContext(Map.empty))
    extraction shouldBe Extracted("a")
  }

  it("should work with user args") {
    import ArgsInstances._
    val result = withArgs((arg[Int]("n"): UserArg[Int], arg[Int]("m")))
    val extraction = result.extract(FnContext(Map("n" -> 5, "m" -> 10)))
    extraction shouldBe Extracted(5 :: 10 :: HNil)
  }

  it("should work with single user arg") {
    import ArgsInstances._
    val result = withArgs(arg[Int]("n"): UserArg[Int])
    val extraction = result.extract(FnContext(Map("n" -> 5, "m" -> 10)))
    extraction shouldBe Extracted(5)
  }

}
