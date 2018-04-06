import mist.api.all._
import org.apache.spark.sql.SparkSession

object SparkSessionExample extends MistFn {

  override def handle: Handle = {
     withArgs(
       arg[Seq[Int]]("numbers"),
       arg[Int]("multiplier", 2)
      ).withMistExtras
       .onSparkSession((nums: Seq[Int], mult: Int, extras: MistExtras, spark: SparkSession) => {
          spark.sparkContext.parallelize(nums).map(_ * mult).collect()
       })
 }

}
