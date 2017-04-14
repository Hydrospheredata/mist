package io.hydrosphere.mist.ml

import java.nio.file.Files

import io.hydrosphere.mist.Constants
import io.hydrosphere.mist.utils.json.ModelMetadataJsonSerialization
import org.apache.spark.ml.{PipelineModel, Transformer}

import scala.io.Source
import TransformerFactory._
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.ml.classification.LogisticRegression
import spray.json.{DeserializationException, pimpString}

import scala.collection.mutable

object ModelLoader extends Logger with ModelMetadataJsonSerialization {

//  {
//    "class":"org.apache.spark.ml.PipelineModel",
//    "timestamp":1480604356248,
//    "sparkVersion":"2.0.0",
//    "uid":"pipeline_5a99d584b039",
//    "paramMap": {
//      "stageUids":["mlpc_c6d88c0182d5"]
//    }
//  }

//  {
//    "class": "org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel",
//    "timestamp": 1480604356363,
//    "sparkVersion": "2.0.0",
//    "uid": "mlpc_c6d88c0182d5",
//    "paramMap": {
//      "featuresCol": "features",
//      "predictionCol": "prediction",
//      "labelCol": "label"
//    }
//  }

//  {
//    "class": "org.apache.spark.ml.feature.HashingTF",
//    "timestamp": 1482134164986,
//    "sparkVersion": "2.0.0",
//    "uid": "hashingTF_faa5eaa6dcbb",
//    "paramMap": {
//      "inputCol": "words",
//      "binary": false,
//      "numFeatures": 1000,
//      "outputCol": "features"
//    }
//  }

//  {
//    "class":"org.apache.spark.ml.classification.DecisionTreeClassificationModel",
//    "timestamp":1486760757329,
//    "sparkVersion":"2.1.0",
//    "uid":"dtc_8582acc89f5a",
//    "paramMap":{
//        "impurity":"gini",
//        "cacheNodeIds":false,
//        "labelCol":"label",
//        "checkpointInterval":10,
//        "predictionCol":"prediction",
//        "maxBins":32,
//        "featuresCol":"features",
//        "seed":159147643,
//        "minInstancesPerNode":1,
//        "rawPredictionCol":"rawPrediction",
//        "probabilityCol":"probability",
//        "minInfoGain":0.0,
//        "maxMemoryInMB":256,
//        "maxDepth":5
//     },
//    "numFeatures":1000,
//    "numClasses":2
//  }
  
  // TODO: tests

  def get(path: String): PipelineModel = {

    // TODO: HDFS support
    val metadata = Source.fromFile(s"$path/metadata/part-00000").mkString
    logger.debug(s"parsing $path/metadata/part-00000")
    logger.debug(metadata)
    try {
      val pipelineParameters = metadata.parseJson.convertTo[Metadata]
      ModelCache.get[PipelineModel](pipelineParameters.uid) match {
        case Some(model) => model
        case None =>
          val stages: Array[Transformer] = getStages(pipelineParameters, path)
          val pipeline = TransformerFactory(pipelineParameters, Map("stages" -> stages.toList)).asInstanceOf[PipelineModel]
          ModelCache.add[PipelineModel](pipeline)
          pipeline
      }
    } catch {
      case exc: DeserializationException =>
        logger.error(s"Deserialization error while parsing pipeline metadata: $exc")
        throw exc
    }
  }

  /**
    * method for parsing model data from "/data/" folder
    *
    * @param pipelineParameters
    * @param path
    * @return
    */
  def getData(pipelineParameters: Metadata, path: String) = ???

  def getStages(pipelineParameters: Metadata, path: String): Array[Transformer] = pipelineParameters.paramMap("stageUids").asInstanceOf[List[String]].zipWithIndex.toArray.map {
    case (uid: String, index: Int) =>
      val currentStage = s"$path/stages/${index}_$uid"
      logger.debug(s"reading $uid stage")
      logger.debug(s"$currentStage/metadata/part-00000")
      val modelMetadata = Source.fromFile(s"$currentStage/metadata/part-00000").mkString
      logger.debug(modelMetadata)
      try {
        val stageParameters = modelMetadata.parseJson.convertTo[Metadata]
        logger.debug(s"Stage class: ${stageParameters.className}")
        ModelCache.get(stageParameters.uid) match {
          case Some(model) => model
          case None =>
            val model = loadTransformer(stageParameters, currentStage)
            ModelCache.add(model)
            model
        }
      } catch {
        case exc: DeserializationException =>
          logger.error(s"Deserialization error while parsing stage metadata: $exc")
          throw exc
      }
  }

  def loadTransformer(stageParameters: Metadata, path: String): Transformer = {
    stageParameters.className match {
      case Constants.ML.Models.randomForestClassifier |
           Constants.ML.Models.gbtRegressor |
           Constants.ML.Models.randomForestRegressor =>
        val data = ModelDataReader.parse(s"$path/data") map {
          case (key: String, value: Any) =>
            key -> value.asInstanceOf[mutable.Map[String, Any]].toMap
        }
        val treesMetadata = ModelDataReader.parse(s"$path/treesMetadata") map {
          case (key: String, value: Any) =>
            val subMap = value.asInstanceOf[Map[String, Any]]
            val metadata = subMap("metadata").toString.parseJson.convertTo[Metadata]
            val treeMeta = Metadata(
              metadata.className,
              metadata.timestamp,
              metadata.sparkVersion,
              metadata.uid,
              metadata.paramMap,
              stageParameters.numFeatures,
              stageParameters.numClasses,
              stageParameters.numTrees
            )
            key -> Map(
              "metadata" -> treeMeta,
              "weights" -> subMap("weights").asInstanceOf[java.lang.Double]
            )
        }

        val newParams = stageParameters.paramMap + ("treesMetadata" -> treesMetadata)
        val newMetadata = Metadata(
          stageParameters.className,
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