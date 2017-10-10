import mist.api.MistJob
import mist.api.DefaultEncoders._
import org.apache.spark.SparkContext

object SimpleContextV2 extends MistJob[Seq[Int]] {

  override def defineJob = (arg[Seq[Int]]("numbers") & arg[Int]("multiplier", 2)).onSparkContext(
    (nums: Seq[Int], mult: Int, sc: SparkContext) => {

      sc.parallelize(nums).map(_ * mult).collect().toSeq
  })

}
