import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object SparkContextExample extends MistJob[Array[Int]] {

  override def defineJob = {
    withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

       import extras._

       logger.info(s"Heello from $jobId")
       sc.parallelize(nums).map(_ * mult).collect()
     })
  }

}
