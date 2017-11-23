import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.sql.SparkSession

object SparkSessionExample extends MistJob[Array[Int]]{

  override def defineJob: JobDef[Array[Int]] = {
     withArgs(
       arg[Seq[Int]]("numbers"),
       arg[Int]("multiplier", 2)
      ).withMistExtras
       .onSparkSession((nums: Seq[Int], mult: Int, extras: MistExtras, spark: SparkSession) => {
          import extras._

          logger.info(s"Heello from $jobId")
          spark.sparkContext.parallelize(nums).map(_ * mult).collect()
       })
 }

}
