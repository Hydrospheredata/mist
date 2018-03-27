import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object SparkContextExample extends MistFn[Array[Int]] {

  override def handle = {
    withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

       import extras._

       logger.info(s"Heello from $jobId")
       sc.parallelize((1 to 100000).flatMap(_ => nums)).map(_ * mult).collect()
       Thread.sleep(1000 * 10)
       sc.parallelize((1 to 100000).flatMap(_ => nums)).map(_ * mult).collect()
     })
  }

}
