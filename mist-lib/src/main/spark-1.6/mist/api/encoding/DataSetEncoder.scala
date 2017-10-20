package mist.api.encoding

import java.util.Locale

import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types._

import mist.api.data._

trait DatasetEncoder {

  // based on org.apache.spark.sql.catalyst.json.JacksonGenerator
  // TODO: UserDefinedType ??
  implicit val datasetEncoder = new Encoder[Dataset[_]] {

    def apply(ds: Dataset[_]): JsLikeData = {
      val rowEncoder = new ShemedRowEncoder(ds.schema)
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

object DatasetEncoder extends DatasetEncoder
