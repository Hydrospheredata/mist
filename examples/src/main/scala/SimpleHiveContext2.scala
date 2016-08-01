/*
import io.hydrosphere.mist.MistJob
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.hive.HiveContext
val versionRegex = "(\\d+)\\.(\\d+).*".r
val sparkVersion = util.Properties.propOrNone("sparkVersion").getOrElse("[1.5.2, )")

val checkSparkSessionLogic = {
  sparkVersion match {
    case versionRegex(major, minor) if major.toInt > 1 => true
    case _ => false
  }
}
if (checkSparkSessionLogic) {


  object SimpleHiveContext2 extends MistJob {
    /** Contains implementation of spark job with ordinary [[org.apache.spark.sql.SQLContext]]
      * Abstract method must be overridden
      *
      * @param context    spark hive context
      * @param parameters user parameters
      * @return result of the job
      */
    override def doStuff(context: SparkSession, parameters: Map[String, Any]): Map[String, Any] = {

      val df = context.read.json(parameters("file").asInstanceOf[String])
      df.printSchema()
      df.registerTempTable("people")

      Map("result" -> context.sql("SELECT AVG(age) AS avg_age FROM people").collect())
    }
  }
}*/