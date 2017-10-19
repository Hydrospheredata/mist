import mist.api._
import mist.api.DefaultEncoders._
import org.apache.spark.SparkContext

object SimpleContextV2 extends MistJob[Array[Int]] {

  override def defineJob = {
    withArgs(
      arg[Seq[Int]]("numbers").validated(_.nonEmpty, "You shall not pass with empty numbers!"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

       import extras._

       logger.info(s"Heello from $jobId")
       sc.parallelize(nums).map(_ * mult).collect()
     })
  }

}
