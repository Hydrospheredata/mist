import com.provectus.mist.MistJob
import org.apache.spark.sql.hive.HiveContext


object HiveContext extends MistJob {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param context    spark hive context
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(context: HiveContext, parameters: Map[String, Any]): Map[String, Any] = {

    val df = context.read.json(parameters("file").asInstanceOf[String])
    df.printSchema()
    df.registerTempTable("people")

    Map("result" -> context.sql("SELECT AVG(age) AS avg_age FROM people").collect())

  }
}