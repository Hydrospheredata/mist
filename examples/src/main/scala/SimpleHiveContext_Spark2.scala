import io.hydrosphere.mist.lib.{HiveSupport, MistJob, SQLSupport}


object SimpleHiveContext extends MistJob with SQLSupport with HiveSupport {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  override def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val df = session.read.json(parameters("file").asInstanceOf[String])
    df.printSchema()
    df.createOrReplaceTempView("people")

    Map("result" -> session.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}
