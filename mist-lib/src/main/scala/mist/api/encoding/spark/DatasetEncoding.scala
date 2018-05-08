package mist.api.encoding.spark

import mist.api.data._
import mist.api.encoding.JsEncoder
import org.apache.spark.sql.Dataset

trait DatasetEncoding {

  implicit val datasetEncoder: JsEncoder[Dataset[_]] = JsEncoder(ds => {
    val rowEncoder = new SchemedRowEncoder(ds.schema)

    val rdd = ds.queryExecution.toRdd.mapPartitions(iter => {
      new Iterator[JsData] {
        override def hasNext: Boolean = iter.hasNext
        override def next(): JsData = {
          val row = iter.next()
          rowEncoder.encode(row)
        }
      }
    })

    JsList(rdd.collect().toSeq)
  })
}

object DatasetEncoding extends DatasetEncoding
