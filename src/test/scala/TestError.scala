import com.provectus.mist.MistJob
import org.apache.spark.SparkContext

object TestError extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.SparkContext]]
    * Abstract method must be overridden
    *
    * @param context    spark context
    * @param parameters user parameters
    * @return result exception Test Error
    */
  override def doStuff(context: SparkContext, parameters: Map[String, Any]): Map[String, Any] = {
    throw new Exception("Test Error")
  }
}