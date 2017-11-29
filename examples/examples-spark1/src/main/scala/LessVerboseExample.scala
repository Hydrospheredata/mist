import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

case class Args(
  numbers: Seq[Int],
  multiplier: Option[Int]
) {

  def mult: Int = multiplier.getOrElse(2)
}

object LessVerboseExample extends MistFn[Array[Int]] {

  import MistExtras._

  override def handler = (arg[Args] & mistExtras).onSparkContext(
    (args: Args, extras: MistExtras, sc: SparkContext) => {
      import extras._

      logger.info(s"Heello from $jobId")
      sc.parallelize(args.numbers).map(_ * args.mult).collect()
    })

}

