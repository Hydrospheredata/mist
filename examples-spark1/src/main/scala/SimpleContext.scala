import io.hydrosphere.mist.api._

object SimpleContext extends MistJob with SQLSupport with MistLogging {

  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param numbers list of int to process
    * @return result of the job
    */
  def execute(numbers: List[Int], multiplier: Option[Int]): Map[String, Any] = {
    val logger = getLogger
    val multiplierValue = multiplier.getOrElse(2)
    val rdd = context.parallelize((1 to 1000).flatMap(x => numbers.map(_ * x)))
    logger.info("HELLLO")
    val x = rdd.map(x => { val z = x * multiplierValue; logger.info(s"SDHSDH $x"); z }).collect()
    Map("result" -> x)
  }
}
