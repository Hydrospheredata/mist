package mist.api.encoding.spark

import mist.api.data.{JsData, JsList}
import mist.api.encoding.JsEncoder
import org.apache.spark.sql.DataFrame

trait DataFrameEncoding {

  implicit val dataFrameEncoder: JsEncoder[DataFrame] = JsEncoder(df => {
    val rowEncoder = new SchemedRowEncoder(df.schema)
    val x = df.queryExecution.toRdd.mapPartitions(iter => {
      new Iterator[JsData] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): JsData = {
          val row = iter.next()
          rowEncoder.encode(row)
        }
      }
    });
    JsList(x.collect().toSeq)
  })

}

object DataFrameEncoding extends DataFrameEncoding
