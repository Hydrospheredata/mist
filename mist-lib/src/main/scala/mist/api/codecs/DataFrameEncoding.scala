package mist.api.codecs

import mist.api.data.{JsLikeData, JsLikeList}
import org.apache.spark.sql.DataFrame

trait DataFrameEncoding {

  implicit val dataFrameEncoder = new Encoder[DataFrame] {
    override def apply(df: DataFrame): JsLikeData = {
      val rowEncoder = new SchemedRowEncoder(df.schema)
      val data = df.queryExecution.toRdd.mapPartitions(iter => {
        new Iterator[JsLikeData] {
          override def hasNext: Boolean = iter.hasNext
          override def next(): JsLikeData = {
            val row = iter.next()
            rowEncoder.encode(row)
          }
        }
      })
      JsLikeList(data.collect().toSeq)
    }
  }

}

object DataFrameEncoding extends DataFrameEncoding
