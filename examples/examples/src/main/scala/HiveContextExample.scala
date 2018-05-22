import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._
import mist.api.encoding.spark._

import org.apache.spark.sql.hive.HiveContext

object HiveContextExample extends MistFn {

  override def handle: Handle = {
    val raw = withArgs(arg[String]("file")).onHiveContext((file: String, hiveCtx: HiveContext) => {
      val df = hiveCtx.read.json(file)
      df.registerTempTable("people")

      hiveCtx.sql("SELECT AVG(age) AS avg_age FROM people")
    })
    raw.asHandle
  }
}
