import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import org.apache.spark.SparkContext

object SparkContextExample extends MistFn with Logging {

  override def handle: Handle = {
    val raw = withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {
       import extras._
       logger.info(s"Heello from $jobId")
       sc.parallelize(nums).map(x => {logger.info("yoo"); x * mult}).collect()
     })
    raw.asHandle
  }

}
