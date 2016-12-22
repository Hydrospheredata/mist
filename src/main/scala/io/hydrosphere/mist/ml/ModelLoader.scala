package io.hydrosphere.mist.ml

import io.hydrosphere.mist.utils.json.ModelMetadataJsonSerialization
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

import scala.io.Source
//import io.circe._
//import io.circe.parser._
//import io.circe.generic.semiauto._
//import cats.syntax.either._
import io.hydrosphere.mist.Logger
import io.hydrosphere.mist.ml.transformers.TransformerFactory

import spray.json.{DeserializationException, pimpString}

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

//  implicit val pipelineParametersDecoder: Decoder[PipelineParameters] = deriveDecoder
//  implicit val stageParametersDecoder: Decoder[StageParameters] = deriveDecoder
  
  def get(path: String): PipelineModel = {

    // TODO: fix replacing 
    val metadata = Source.fromFile(s"$path/metadata/part-00000").mkString.replace(""""class"""", """"className"""")
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
      case e: DeserializationException =>
        logger.error(s"Deserialization error while parsing pipeline metadata: $e")
        // TODO: null! 
        null
    }
  }
  
  def getStages(pipelineParameters: Metadata, path: String): Array[Transformer] = pipelineParameters.paramMap("stageUids").asInstanceOf[List[String]].zipWithIndex.toArray.map {
    case (uid: String, index: Int) =>
      logger.debug(s"reading $uid stage")
      logger.debug(s"$path/stages/${index}_$uid/metadata/part-00000")
      val modelMetadata = Source.fromFile(s"$path/stages/${index}_$uid/metadata/part-00000").mkString.replace(""""class"""", """"className"""")
      logger.debug(modelMetadata)
      try {
        val stageParameters = modelMetadata.parseJson.convertTo[Metadata]
        logger.debug(s"Stage class: ${stageParameters.className}")
        ModelCache.get(stageParameters.uid) match {
          case Some(model) => model
          case None =>
            val data = ModelDataReader.parse(s"$path/stages/${index}_$uid/data/")
            val model = TransformerFactory(stageParameters, data)
            // TODO: check if files were changed 
            ModelCache.add(model)
            model
        }
      } catch {
        case e: DeserializationException =>
          logger.error(s"Deserialization error while parsing stage metadata: $e")
          // TODO: null!
          null
      }
  }
  
}