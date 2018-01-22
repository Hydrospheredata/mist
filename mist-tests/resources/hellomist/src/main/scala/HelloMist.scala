import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext

object HelloMist extends MistFn[Double] {

  override def handle = {
    withArgs(
      arg[Int]("samples", 1000)
    )
    .withMistExtras
    .onSparkContext((n: Int, extras: MistExtras, sc: SparkContext) => {
      import extras._

      logger.info(s"Hello Mist started with samples: $n")

      val count = sc.parallelize(1 to n).filter(_ => {
        val x = math.random
        val y = math.random
        x * x + y * y < 1
      }).count()

      val pi = (4.0 * count) / n
      pi
    })
  }
}
