package io.hydrosphere.mist.ml

import java.io.File

import io.hydrosphere.mist.utils.parquet.{SimpleReadSupport, SimpleRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import parquet.hadoop.{ParquetFileReader, ParquetReader}
import parquet.schema.MessageType

import scala.collection.immutable.HashMap
import scala.collection.mutable
object ModelDataReader {
  def parse(path: String): Map[String, Any] = {
    val parquetFileOption = Option(new File(path).listFiles).map(_.filter(_.isFile).toList.find(_.getAbsolutePath.endsWith("parquet")).orNull)
    parquetFileOption match {
      case Some(parquetFile) =>
        val conf: Configuration = new Configuration()
        val metaData = ParquetFileReader.readFooter(conf, new Path(parquetFile.getAbsolutePath), NO_FILTER)
        val schema: MessageType = metaData.getFileMetaData.getSchema

        val reader: ParquetReader[SimpleRecord] = ParquetReader.builder[SimpleRecord](new SimpleReadSupport(), new Path(path)).build()
        val result = mutable.HashMap.empty[String, Any]
        try {
          var value = reader.read()
          while (value != null) {
            value.prettyPrint(schema)
            val valMap = value.struct(HashMap.empty[String, Any], schema)
            mergeMaps(result, valMap)
            value = reader.read()
          }
          HashMap(result.toSeq:_*)
        } finally {
          if (reader != null) {
            reader.close()
          }
        }
      case None =>
        new HashMap[String, Any]
    }
  }

  // TODO ugly
  private def mergeMaps(acc: mutable.HashMap[String, Any], map: HashMap[String, Any]): Unit = {
    if (map.contains("leftChild") && map.contains("rightChild") && map.contains("id")) { // tree structure detected
      acc += map("id").toString -> map
    } else if (map.contains("treeID") && map.contains("nodeData")) { // ensemble structure detected
      if (!acc.contains(map("treeID").toString)) {
        acc += map("treeID").toString -> mutable.Map.empty[String, Map[String, Any]]
      }
      val nodes = acc(map("treeID").toString)
        .asInstanceOf[mutable.Map[String, Map[String, Any]]]
      val nodeData = map("nodeData").asInstanceOf[Map[String, Any]]
      nodes += nodeData("id").toString -> nodeData
    } else if (map.contains("treeID") && map.contains("metadata")) { // ensemble metadata structure detected
      acc += map("treeID").toString -> map
    } else if (map.contains("clusterIdx") && map.contains("clusterCenter")) { // clusters detected
      acc += map("clusterIdx").toString -> map("clusterCenter")
    } else {
      acc ++= map
    }
  }
}