import io.hydrosphere.mist.api._
import org.apache.spark.sql.SparkSession


object SimpleHiveContext extends MistJob {
  def session: SparkSession = SparkSession
    .builder()
    .appName(context.appName)
    .config(context.getConf)
    .getOrCreate()

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

    val result = session.sql("SELECT AVG(age) AS avg_age FROM people")
      .collect()
      .map(r => r.getDouble(0).toString)

    Map("result" -> result)
  }
}
