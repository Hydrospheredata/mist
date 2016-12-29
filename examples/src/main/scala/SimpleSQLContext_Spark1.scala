import io.hydrosphere.mist.lib.{MistJob, SQLSupport}

object SimpleSQLContext extends MistJob with SQLSupport {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {
    val df = sqlContext.read.json(parameters("file").asInstanceOf[String])
    df.registerTempTable("people")
    Map("result" -> sqlContext.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}
