import mist.api._
import mist.api.MistExtras
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging

object SparkContextExample extends MistFn[Array[Int]] with Logging {
  type X[A] = Handle2[A, _]


  override def handle = {

    //val x: X[Nothing] =
    val x: Handle2[Nothing, _] =
      withArgs(
        arg[Seq[Int]]("numbers"),
        arg[Int]("multiplier", 2)
      ).withMistExtras
        .onSparkContext2((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

          import extras._

          logInfo(s"Heello from $jobId")
          sc.parallelize(nums).map(_ * mult).collect()
        })

    val x1 = withArgs(
      arg[Seq[Int]]("numbers"),
      arg[Int]("multiplier", 2)
    ).withMistExtras
     .onSparkContext((nums: Seq[Int], mult: Int, extras: MistExtras, sc: SparkContext) => {

       import extras._

       logInfo(s"Heello from $jobId")
       sc.parallelize(nums).map(_ * mult).collect()
     })

    val x2 = withArgs(
      arg[Seq[Int]]("numbers")
    ).withMistExtras
      .onSparkContext((nums: Seq[Int], extras: MistExtras, sc: SparkContext) => {

        import extras._

        logInfo(s"Heello from $jobId")
        sc.parallelize(nums).map(_ * 2).collect()
      })
  }



}
