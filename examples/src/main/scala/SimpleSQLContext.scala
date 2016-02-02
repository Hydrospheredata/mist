import com.provectus.lymph.LymphJob
import org.apache.spark.sql.SQLContext

object SimpleSQLContext extends LymphJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param context    spark sql context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: SQLContext, parameters: Map[String, Any]): Map[String, Any] = {
    val df = context.read.json(parameters("file").asInstanceOf[String])
    df.registerTempTable("people")
    Map("result" -> context.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}