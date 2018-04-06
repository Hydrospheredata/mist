import mist.api.all._
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

object SparkContextExample extends MistFn with Logging {

  override def handle: Handle = {
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
