import mist.api.all._
import mist.api.encoding.DataFrameEncoding._
import mist.api.encoding.DefaultExtractorInstances._
import mist.api.encoding.DefaultEncoderInstances._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

object HiveContextExample extends MistFn {

  override def handle: Handle = {
    withArgs(arg[String]("file")).onHiveContext((file: String, hiveCtx: HiveContext) => {
      val df = hiveCtx.read.json(file)
      df.registerTempTable("people")

      hiveCtx.sql("SELECT AVG(age) AS avg_age FROM people")
    })
  }
}
