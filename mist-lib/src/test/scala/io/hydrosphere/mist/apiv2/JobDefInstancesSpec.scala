package io.hydrosphere.mist.apiv2

import org.apache.spark.SparkContext
import org.scalatest._

class JobDefInstancesSpec extends FunSpec with Matchers {

  import JobDefInstances._

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

  it("for all args") {
    allArgs.extract(testCtx("a" -> "b", "x" -> 42)) shouldBe
      Extracted(Map("a" -> "b", "x" -> 42))

    allArgs.extract(testCtx()) shouldBe Extracted(Map.empty)
  }

  it("for spark context") {
    val spJob = arg[Seq[Int]]("nums").onSparkContext.apply((nums: Seq[Int], sp: SparkContext) => {
      sp.parallelize(nums).map(_ * 2).collect()
    })
    spJob.invoke(testCtx("nums" -> (1 to 10)))
  }


  def testCtx(params: (String, Any)*): JobContext = {
    JobContext(null, params.toMap)
  }

}
