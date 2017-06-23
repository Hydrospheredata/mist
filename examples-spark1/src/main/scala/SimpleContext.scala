import io.hydrosphere.mist.api._

object SimpleContext extends MistJob with Logging {

  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param numbers list of int to process
    * @return result of the job
    */
  def execute(numbers: List[Int], multiplier: Option[Int]): Map[String, Any] = {
    val logger = getLogger
    val multiplierValue = multiplier.getOrElse(2)
    val rdd = context.parallelize(numbers)
    logger.info(s"Hello from SimpleContext")
    val x = rdd.map(x => x * multiplierValue).collect()
    Map("result" -> x)
  }
}
