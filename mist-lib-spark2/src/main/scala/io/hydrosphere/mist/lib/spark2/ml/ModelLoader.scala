package io.hydrosphere.mist.lib.spark2.ml

import org.apache.spark.ml.{PipelineModel, Transformer}
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable
import scala.io.Source

// TODO: tests
// TODO: HDFS support
object ModelLoader {

  private val RandomForestClassifier = "org.apache.spark.ml.classification.RandomForestClassificationModel"

  implicit val formats = DefaultFormats

  def get(path: String): PipelineModel = {
    val metadata = Source.fromFile(s"$path/metadata/part-00000").mkString
    val pipelineParameters = parse(metadata).extract[Metadata]
    val stages: Array[Transformer] = getStages(pipelineParameters, path)
    val pipeline = TransformerFactory(pipelineParameters, Map("stages" -> stages.toList)).asInstanceOf[PipelineModel]
    pipeline
  }

  /**
    * method for parsing model data from "/data/" folder
    *
    * @param pipelineParameters
    * @param path
    * @return
    */
  def getData(pipelineParameters: Metadata, path: String) = ???

  def getStages(pipelineParameters: Metadata, path: String): Array[Transformer] =
    pipelineParameters.paramMap("stageUids").asInstanceOf[List[String]].zipWithIndex.toArray.map {
      case (uid: String, index: Int) =>
        val currentStage = s"$path/stages/${index}_$uid"
        val modelMetadata = Source.fromFile(s"$currentStage/metadata/part-00000").mkString
        val stageParameters = parse(modelMetadata).extract[Metadata]
        loadTransformer(stageParameters, currentStage)
    }

  def loadTransformer(stageParameters: Metadata, path: String): Transformer = {
    stageParameters.`class` match {
      case RandomForestClassifier =>
        val data = ModelDataReader.parse(s"$path/data") map { kv =>
          kv._1 -> kv._2.asInstanceOf[mutable.Map[String, Any]].toMap
        }
        val treesMetadata = ModelDataReader.parse(s"$path/treesMetadata") map {kv =>
          val subMap = kv._2.asInstanceOf[Map[String, Any]]
          val content = subMap("metadata").toString
          val metadata = parse(content).extract[Metadata]
          val treeMeta = Metadata(
            metadata.`class`,
            metadata.timestamp,
            metadata.sparkVersion,
            metadata.uid,
            metadata.paramMap,
            stageParameters.numFeatures,
            stageParameters.numClasses,
            stageParameters.numTrees
          )
          kv._1 -> Map(
            "metadata" -> treeMeta,
            "weights" -> subMap("weights").asInstanceOf[java.lang.Double]
          )
        }
        val newParams = stageParameters.paramMap + ("treesMetadata" -> treesMetadata)
        val newMetadata = Metadata(
          stageParameters.`class`,
          stageParameters.timestamp,
          stageParameters.sparkVersion,
          stageParameters.uid,
          newParams,
          stageParameters.numFeatures,
          stageParameters.numClasses,
          stageParameters.numTrees
        )
        TransformerFactory(newMetadata, data)
      case _ =>
        val data = ModelDataReader.parse(s"$path/data/")
        TransformerFactory(stageParameters, data)
    }
  }

}
