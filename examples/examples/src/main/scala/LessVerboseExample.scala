import mist.api.all._
import mist.api.args.ArgExtractor
import org.apache.spark.SparkContext

case class Args(
  numbers: Seq[Int],
  multiplier: Option[Int]
) {

  def mult: Int = multiplier.getOrElse(2)
}

object LessVerboseExample extends MistFn {

  implicit val extractor = ArgExtractor.rootFor[Args]

  override def handle: Handle = arg[Args].onSparkContext((args: Args, sc: SparkContext) => {
    sc.parallelize(args.numbers).map(_ * args.mult).collect()
  })

}

