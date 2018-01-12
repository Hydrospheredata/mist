import mist.api._
import mist.api.MistExtras
import mist.api.args.ArgExtractor
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

case class NumArg(value: Int, mult: Int)
case class Args(
  numbers: Seq[NumArg]
)

object LessVerboseExample extends MistFn[Array[Int]] {

  import MistExtras._

  implicit val argExt = ArgExtractor.rootFor[Args]

  override def handle = (arg[Args] & mistExtras).onSparkContext(
    (args: Args, extras: MistExtras, sc: SparkContext) => {
      import extras._

      logger.info(s"Heello from $jobId")
      //sc.parallelize(args.numbers).map(a => a.value * a.mult).collect()
      Array.empty[Int]
    })

}

