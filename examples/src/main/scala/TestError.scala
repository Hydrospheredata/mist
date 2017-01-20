import io.hydrosphere.mist.lib.MistJob

object TestError extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result exception Test Error
    */
  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    throw new Exception("Test Error")
  }
}