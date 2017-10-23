package mist.api.encoding

import mist.api.data._
import org.apache.spark.sql.Dataset

trait DatasetEncoding {

  implicit val datasetEncoder = new Encoder[Dataset[_]] {

    def apply(ds: Dataset[_]): JsLikeData = {
      val rowEncoder = new SchemedRowEncoder(ds.schema)
      val data = ds.queryExecution.toRdd.mapPartitions(iter => {
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

object DatasetEncoding extends DatasetEncoding
