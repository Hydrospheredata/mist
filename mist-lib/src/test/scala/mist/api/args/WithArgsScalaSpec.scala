package mist.api.args

import mist.api.{ArgDef, Extracted, JobContext}
import org.scalatest.{FunSpec, Matchers}
import shapeless.{::, HNil}

class WithArgsScalaSpec extends FunSpec with Matchers {

  import WithArgsScala._
  import ArgDef._

  it("should apply tuples") {
    val result = withArgs(const("a"), const("b"), const("c"), const(5), const("last"))
    val extraction = result.extract(JobContext(Map.empty))
    extraction shouldBe Extracted("a" :: "b" :: "c" :: 5 :: "last" :: HNil)
  }

  it("should apply single element") {
    val result = withArgs(const("a"))
    val extraction = result.extract(JobContext(Map.empty))
    extraction shouldBe Extracted("a")
  }

}
