import mist.api.all._
import mist.api.encoding.DefaultExtractorInstances._
import mist.api.encoding.DefaultEncoderInstances._
import org.apache.spark.sql.SQLContext

object SQLContextExample extends MistFn {

  override def handle: Handle = {
    withArgs(arg[String]("file")).onSqlContext((file: String, sqlCtx: SQLContext) => {
      val df = sqlCtx.read.json(file)
      df.registerTempTable("people")

      sqlCtx.sql("SELECT AVG(age) AS avg_age FROM people")
        .collect()
        .map(r => r.getDouble(0).toInt)
        .toSeq
    })
  }
}
