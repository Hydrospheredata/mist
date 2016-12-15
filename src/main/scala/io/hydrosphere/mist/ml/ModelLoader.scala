package io.hydrosphere.mist.ml

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.collection.immutable.HashMap
import scala.io.Source

import io.circe._
import io.circe.parser._
import io.circe.generic.semiauto._
import cats.syntax.either._

object ModelLoader {

  case class PipelineParameters(className: String, timestamp: Long, sparkVersion: String, uid: String, paramMap: Map[String, List[String]])

  //  {
  //    "class":"org.apache.spark.ml.PipelineModel",
  //    "timestamp":1480604356248,
  //    "sparkVersion":"2.0.0",
  //    "uid":"pipeline_5a99d584b039",
  //    "paramMap": {
  //      "stageUids":["mlpc_c6d88c0182d5"]
  //    }
  //  }

  case class StageParameters(className: String, timestamp: Long, sparkVersion: String, uid: String, paramMap: Map[String, String])

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


  implicit val pipelineParametersDecoder: Decoder[PipelineParameters] = deriveDecoder
  implicit val stageParametersDecoder: Decoder[StageParameters] = deriveDecoder

  def get(path: String): Array[Transformer] = {
    ModelCache.get[MultilayerPerceptronClassificationModel](path) match {
      case Some(model) => Array(model)
      case None =>
        val metadata = Source.fromFile(s"$path/metadata/part-00000").mkString.replace("class", "className")
        println(s"$path/metadata/part-00000")
        println(metadata)
        val parsed = parse(metadata).getOrElse(Json.Null).as[PipelineParameters]
        parsed match {
          case Left(failure) =>
            println(s"FAILURE while parsing pipeline metadata json: $failure")
            null
          case Right(pipelineParameters: PipelineParameters) =>
            pipelineParameters.paramMap("stageUids").zipWithIndex.toArray.map {
              case (uid: String, index: Int) =>
                println(s"reading $uid stage")
                println(s"$path/stages/${index}_$uid/metadata/part-00000")
                val modelMetadata = Source.fromFile(s"$path/stages/${index}_$uid/metadata/part-00000").mkString.replace(""""class"""", """"className"""")
                println(modelMetadata)
                val parsedModel = parse(modelMetadata).getOrElse(Json.Null).as[StageParameters]
                parsedModel match {
                  case Left(failure) =>
                    println(s"FAILURE while parsing stage $uid metadata json: $failure")
                    null
                  case Right(stageParameters: StageParameters) =>
                    println(s"Stage class: ${stageParameters.className}")
                    val data = ModelDataReader.parse(s"$path/stages/${index}_$uid/data/")

                    val cls = Class.forName(stageParameters.className)
                    val constructor = cls.getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])

                    constructor.setAccessible(true)
                    val now = System.currentTimeMillis
                    val res = constructor.newInstance(uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[HashMap[String, Any]]("values").asInstanceOf[List[Double]].toArray)).asInstanceOf[MultilayerPerceptronClassificationModel]
                    println(s"instantiating: ${System.currentTimeMillis - now}ms")
                    ModelCache.add[MultilayerPerceptronClassificationModel](path, res)
                    res
                }
            }
        }
    }
  }

}