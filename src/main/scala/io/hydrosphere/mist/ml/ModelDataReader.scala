package io.hydrosphere.mist.ml

import java.io.File

import io.hydrosphere.mist.utils.parquet.{SimpleReadSupport, SimpleRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import parquet.hadoop.{ParquetFileReader, ParquetReader}
import parquet.schema.MessageType

import scala.collection.immutable.HashMap

object ModelDataReader {
  def parse(path: String): HashMap[String, Any] = {
    val parquetFile = new File(path).listFiles.filter(_.isFile).toList.filter(_.getAbsolutePath.endsWith("parquet")).head.getAbsolutePath
    val conf: Configuration = new Configuration()
    val metaData = ParquetFileReader.readFooter(conf, new Path(parquetFile), NO_FILTER)
    val schema: MessageType = metaData.getFileMetaData.getSchema

    val reader: ParquetReader[SimpleRecord] = ParquetReader.builder[SimpleRecord](new SimpleReadSupport(), new Path(path)).build()
    var result = HashMap.empty[String, Any]
    try {
      var value = reader.read()
      while (value != null) {
        println(s"new value: ${value.values.length} rows")
        value.prettyPrint(schema)
        result ++= value.struct(HashMap.empty[String, Any], schema)
        value = reader.read()
      }
      result
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }
}
