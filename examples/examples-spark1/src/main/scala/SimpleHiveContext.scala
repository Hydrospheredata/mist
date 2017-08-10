import io.hydrosphere.mist.api.{HiveSupport, MistJob, SQLSupport}

object SimpleHiveContext extends MistJob with SQLSupport with HiveSupport {

  /** Contains implementation of spark job with [[org.apache.spark.sql.HiveContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  def execute(file: String): Map[String, Any] = {
    val df = hiveContext.read.json(file)
    df.printSchema()
    df.registerTempTable("people")
    Map("result" -> hiveContext.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}
