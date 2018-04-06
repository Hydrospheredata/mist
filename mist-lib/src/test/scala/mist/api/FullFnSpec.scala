package mist.api

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.scalatest.FunSpec

object TestExample extends MistFn[Array[Int]] with Logging {

  override def handle: LowHandle[Array[Int]] = {
    withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
      .onSparkContext2((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

        import extras._

        logInfo(s"Heello from $jobId")
        sc.parallelize(nums).map(_ * mult).collect()
      })
  }



}

class FullFnSpec extends FunSpec with TestSparkContext {

  it("should provide test call") {
    val x = TestExample.handle.toRawCaller.apply(Seq(1,2,3))

  }
}
