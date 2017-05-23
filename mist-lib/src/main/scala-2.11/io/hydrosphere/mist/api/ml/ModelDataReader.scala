package io.hydrosphere.mist.api.ml

import java.io.File

import io.hydrosphere.mist.api.ml.reader._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import parquet.format.converter.ParquetMetadataConverter.NO_FILTER
import parquet.hadoop.{ParquetFileReader, ParquetReader}
import parquet.schema.MessageType

import scala.collection.immutable.HashMap
import scala.collection.mutable
object ModelDataReader {

  def parse(path: String): Map[String, Any] = {
    findFile(path) match {
      case Some(f) => readData(f)
      case None => Map.empty
    }
  }

  private def readData(f: File): Map[String, Any] = {
    val conf: Configuration = new Configuration()
    val metaData = ParquetFileReader.readFooter(conf, new Path(f.getAbsolutePath), NO_FILTER)
    val schema: MessageType = metaData.getFileMetaData.getSchema

    val reader = ParquetReader.builder[SimpleRecord](new SimpleReadSupport(), new Path(f.getParent)).build()
    val result = mutable.HashMap.empty[String, Any]


    try {
      var value = reader.read()
      while (value != null) {
        val valMap = value.struct(HashMap.empty[String, Any], schema)
        mergeMaps(result, valMap)
        value = reader.read()
      }
      result.toMap
    } finally {
      if (reader != null) {
        reader.close()
      }
    }
  }

  private def findFile(dataDir: String): Option[File] = {
    val dir = new File(dataDir)
    for {
      childs <- Option(dir.listFiles)
      data <- childs.find(f => f.isFile && f.getName.endsWith(".parquet"))
    } yield data
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