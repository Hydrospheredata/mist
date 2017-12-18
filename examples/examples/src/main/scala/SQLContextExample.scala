import mist.api._
import mist.api.encoding.DefaultEncoders._
import org.apache.spark.sql.SQLContext

object SQLContextExample extends MistFn[Array[Int]]{

  override def handle: Handle[Array[Int]] = {
    withArgs(arg[String]("file")).onSqlContext((file: String, sqlCtx: SQLContext) => {
      val df = sqlCtx.read.json(file)
      df.registerTempTable("people")

      sqlCtx.sql("SELECT AVG(age) AS avg_age FROM people")
        .collect()
        .map(r => r.getDouble(0).toInt)
    })
  }
}
