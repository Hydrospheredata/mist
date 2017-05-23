package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable

class LocalCountVectorizerModel(override val sparkTransformer: CountVectorizerModel) extends LocalTransformer[CountVectorizerModel] {
  override def transform(localData: LocalData): LocalData = {
    val dict = sparkTransformer.vocabulary.zipWithIndex.toMap
    val minTf = sparkTransformer.getMinTF

    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newCol = column.data.map { data =>
          val termCounts = mutable.HashMap.empty[Int, Double]
          var tokenCount = 0L
          val arr = data.asInstanceOf[List[String]]
          arr.foreach { token =>
            dict.get(token) foreach  { index =>
              val storedValue = termCounts.getOrElseUpdate(index, 0.0)
              termCounts.update(index, storedValue + 1.0)
            }
            tokenCount += 1
          }
          val eTF = if (minTf >= 1.0) minTf else tokenCount * minTf
          val eCounts = if (sparkTransformer.getBinary) {
            termCounts filter(_._2 >= eTF) map(_._1 -> 1.0) toSeq
          } else {
            termCounts filter(_._2 >= eTF) toSeq
          }

          Vectors.sparse(dict.size, eCounts.toList)
        }
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newCol))
      case None => localData
    }
  }
}

object LocalCountVectorizerModel extends LocalModel[CountVectorizerModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): CountVectorizerModel = {
    val vocabulary = data("vocabulary").asInstanceOf[List[String]].toArray
    val inst = new CountVectorizerModel(metadata.uid, vocabulary)
    inst
      .setInputCol(metadata.paramMap("inputCol").toString)
      .setOutputCol(metadata.paramMap("outputCol").toString)
      .set(inst.binary, metadata.paramMap("binary").asInstanceOf[Boolean])
      .set(inst.minDF, metadata.paramMap("minDF").toString.toDouble)
      .set(inst.minTF, metadata.paramMap("minTF").toString.toDouble)
      .set(inst.vocabSize, metadata.paramMap("vocabSize").asInstanceOf[Number].intValue())
  }

  override implicit def getTransformer(transformer: CountVectorizerModel): LocalTransformer[CountVectorizerModel] = new LocalCountVectorizerModel(transformer)
}
