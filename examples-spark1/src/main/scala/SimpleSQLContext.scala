import io.hydrosphere.mist.api.{MistJob, SQLSupport}

object SimpleSQLContext extends MistJob with SQLSupport {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param file json file path
    * @return result of the job
    */
  def execute(file: String): Map[String, Any] = {
    val df = sqlContext.read.json(file)
    df.registerTempTable("people")
    Map("result" -> sqlContext.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}
