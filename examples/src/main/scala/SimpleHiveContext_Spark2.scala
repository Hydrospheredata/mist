import io.hydrosphere.mist.lib.{HiveSupport, MistJob, SQLSupport}


object SimpleHiveContext extends MistJob with SQLSupport with HiveSupport {
  /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
    * Abstract method must be overridden
    *
    * @param file json file path
    * @return result of the job
    */
  def execute(file: String): Map[String, Any] = {

    val df = session.read.json(file)
    df.printSchema()
    df.createOrReplaceTempView("people")

    Map("result" -> session.sql("SELECT AVG(age) AS avg_age FROM people").collect())
  }
}
