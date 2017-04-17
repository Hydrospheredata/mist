import io.hydrosphere.mist.lib.spark1.MistJob

object SimpleContext extends MistJob {

  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param numbers list of int to process
    * @return result of the job
    */
  def execute(numbers: List[Int], multiplier: Option[Int]): Map[String, Any] = {
    val multiplierValue = multiplier.getOrElse(2)
    val rdd = context.parallelize(numbers)
    Thread.sleep(1000 * 100)
    Map("result" -> rdd.map(x => x * multiplierValue).collect())
  }
}
