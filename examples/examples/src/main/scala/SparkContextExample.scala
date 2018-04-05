import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

object SparkContextExample extends MistFn[Array[Int]] with Logging {

  override def handle = {
    withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

       import extras._

       logInfo(s"Heello from $jobId")
       sc.parallelize(nums).map(_ * mult).collect()
     })
  }



}
