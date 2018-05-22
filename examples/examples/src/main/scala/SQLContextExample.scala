import mist.api._
import mist.api.dsl._
import mist.api.encoding.defaults._

import org.apache.spark.sql.SQLContext

object SQLContextExample extends MistFn {

  override def handle: Handle = {
    val raw = withArgs(arg[String]("file")).onSqlContext((file: String, sqlCtx: SQLContext) => {
      val df = sqlCtx.read.json(file)
      df.registerTempTable("people")

      sqlCtx.sql("SELECT AVG(age) AS avg_age FROM people")
        .collect()
        .map(r => r.getDouble(0).toInt)
        .toSeq
    })
    raw.asHandle
  }
}
