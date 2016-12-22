package io.hydrosphere.mist.ml

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.mllib.feature.{HashingTF => HTF}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.mllib.linalg.{SparseVector => SVector}

import scala.collection.mutable
import scala.language.implicitConversions

object HackedSpark {

  implicit class HackedTransformer(val transformer: Transformer) {
    def transform(localData: LocalData): LocalData = {
      println(s"transform: ${getClass.getCanonicalName}")
      localData
    }
  }
  
  implicit class HackedPipeline(val pipeline: PipelineModel) {
    
    def transform(localData: LocalData): LocalData = {
      pipeline.stages.foldLeft(localData)((x: LocalData, y: Transformer) => y match {
        case tokenizer: Tokenizer => tokenizer.transform(x)
        case hashingTF: HashingTF => hashingTF.transform(x)
        case logisticRegression: LogisticRegressionModel => logisticRegression.transform(x)
        case _ => throw new Exception(s"Unknown pipeline stage: ${y.getClass}")
      })
    }
    
  }

  implicit class HackedTokenizer(val tokenizer: Tokenizer) {
    def transform(localData: LocalData): LocalData = {
      println(s"Tokenizer")
      println(localData)
      localData.column(tokenizer.getInputCol) match {
        case Some(column) =>
          val method = classOf[Tokenizer].getMethod("createTransformFunc")
          val newData = column.data.map(s => {
            method.invoke(tokenizer).asInstanceOf[String => Seq[String]](s.asInstanceOf[String])
          })
          localData.withColumn(LocalDataColumn(tokenizer.getOutputCol, newData))
        case None => localData
      }
    }
  }
  
  implicit class HackedHashingTF(val hashingTF: HashingTF) {
    def transform(localData: LocalData): LocalData = {
      println(s"HashingTF")
      println(localData)
      localData.column(hashingTF.getInputCol) match {
        case Some(column) =>
          val htf = new HTF(hashingTF.getNumFeatures).setBinary(hashingTF.getBinary)
          val newData = column.data.map((m) => htf.transform(m.asInstanceOf[mutable.WrappedArray[String]]))
          localData.withColumn(LocalDataColumn(hashingTF.getOutputCol, newData))
        case None => localData
      }
    }
  }
  
  implicit class HackedLogisticRegression(val logisticRegression: LogisticRegressionModel) {

    implicit def mllibVectorToMlVector(v: SVector): SparseVector = new SparseVector(v.size, v.indices, v.values)
    
    def transform(localData: LocalData): LocalData = {
      println("LogisticRegression")
      println(localData)
      localData.column(logisticRegression.getFeaturesCol) match {
        case Some(column) =>
          var newData = localData
          val predict = classOf[LogisticRegressionModel].getMethod("predict", classOf[Vector])
          if (logisticRegression.getPredictionCol.nonEmpty) {
            val newColumn = LocalDataColumn(logisticRegression.getPredictionCol, column.data.map(m => {
              val vector: SparseVector = m.asInstanceOf[SVector]
              predict.invoke(logisticRegression, vector)
            }))
            newData = newData.withColumn(newColumn)
          }
          val predictRaw = classOf[LogisticRegressionModel].getMethod("predictRaw", classOf[Vector])
          if (logisticRegression.getRawPredictionCol.nonEmpty) {
            val newColumn = LocalDataColumn(logisticRegression.getRawPredictionCol, column.data.map(m => {
              val vector: SparseVector = m.asInstanceOf[SVector]
              predictRaw.invoke(logisticRegression, vector)
            }))
            newData = newData.withColumn(newColumn)
          }
          val predictProbability = classOf[LogisticRegressionModel].getMethod("predictProbability", classOf[AnyRef])
          if (logisticRegression.getProbabilityCol.nonEmpty) {
            val newColumn = LocalDataColumn(logisticRegression.getProbabilityCol, column.data.map(m => {
              val vector: SparseVector = m.asInstanceOf[SVector]
              predictProbability.invoke(logisticRegression, vector)
            }))
            newData = newData.withColumn(newColumn)
          }
          newData
        case None => localData
      }
    }
  }

}
