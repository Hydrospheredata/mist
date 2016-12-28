import io.hydrosphere.mist.lib.{HiveSupport, MistJob, SQLSupport}
import org.apache.spark.sql.Row

object SimpleHiveContext extends MistJob with SQLSupport with HiveSupport {
  /** Contains implementation of spark job with [[org.apache.spark.sql.HiveContext]]
    * Abstract method must be overridden
    *
    * @param parameters user parameters
    * @return result of the job
    */
  def doStuff(parameters: Map[String, Any]): Map[String, Any] = {

    val df = hiveContext.read.json(parameters("file").asInstanceOf[String])
    df.printSchema()
    df.registerTempTable("people")
    val result = hiveContext.sql("SELECT AVG(age) AS avg_age FROM people").collect().map(_.toSeq)
    Map("result" -> result)

  }
}