package io.hydrosphere.mist.ml.transformers

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.feature.{HashingTF, StringIndexerModel, Tokenizer}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.mllib.feature.{HashingTF => HTF}
import org.apache.spark.mllib.linalg.{SparseVector => SVector}
import org.apache.spark.mllib.feature

import scala.language.implicitConversions
import scala.collection.mutable

object LocalTransformers extends Logger {

  implicit class LocalPipeline(val pipeline: PipelineModel) {

    def transform(localData: LocalData): LocalData = {
      pipeline.stages.foldLeft(localData)((x: LocalData, y: Transformer) => y match {
        case tokenizer: Tokenizer => tokenizer.transform(x)
        case hashingTF: HashingTF => hashingTF.transform(x)
        case strIndexer: StringIndexerModel => strIndexer.transform(x)
        case logisticRegression: LogisticRegressionModel => logisticRegression.transform(x)
        case perceptron: MultilayerPerceptronClassificationModel => perceptron.transform(x)
        case classTree: DecisionTreeClassificationModel => classTree.transform(x)
        case _ => throw new Exception(s"Unknown pipeline stage: ${y.getClass}")
      })
    }

  }

  implicit class LocalDecisionTreeClassificationModel(val tree: DecisionTreeClassificationModel) {
    import io.hydrosphere.mist.ml.DataUtils._

    def transform(localData: LocalData): LocalData = {
      logger.info(s"Local DecisionTreeClassificationModel")
      logger.info(localData.toString)
      localData.column(tree.getFeaturesCol) match {
        case Some(column) =>
          val method = classOf[DecisionTreeClassificationModel].getMethod("predict", classOf[Vector])
          method.setAccessible(true)
          val newColumn = LocalDataColumn(tree.getPredictionCol, column.data map { feature =>
            val vector: SparseVector = feature.asInstanceOf[SVector]
            method.invoke(tree, vector).asInstanceOf[Double]
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }
  
  implicit class LocalTokenizer(val tokenizer: Tokenizer) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local Tokenizer")
      logger.debug(localData.toString)
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

  implicit class LocalHashingTF(val hashingTF: HashingTF) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local HashingTF")
      logger.debug(localData.toString)
      localData.column(hashingTF.getInputCol) match {
        case Some(column) =>
          val htf = new HTF(hashingTF.getNumFeatures).setBinary(hashingTF.getBinary)
          val newData = column.data.map((m) => htf.transform(m.asInstanceOf[mutable.WrappedArray[String]]))
          localData.withColumn(LocalDataColumn(hashingTF.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalStringIndexer(val strIndexer: StringIndexerModel) {
    def transform(localData: LocalData): LocalData = {
      logger.info(s"Local StringIndexer")
      logger.info(localData.toString)
      localData.column(strIndexer.getInputCol) match {
        case Some(column) =>
          val newColumn = LocalDataColumn(strIndexer.getOutputCol, column.data map { feature =>
            strIndexer.transform()
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }

  implicit class LocalMultilayerPerceptronClassificationModel(val perceptron: MultilayerPerceptronClassificationModel) {

    def transform(localData: LocalData): LocalData = {
      logger.debug("Local MultilayerPerceptronClassificationModel")
      logger.debug(localData.toString)
      localData.column(perceptron.getFeaturesCol) match {
        case Some(column) =>
          val method = classOf[MultilayerPerceptronClassificationModel].getMethod("predict", classOf[Vector])
          method.setAccessible(true)
          val newColumn = LocalDataColumn(perceptron.getPredictionCol, column.data map { feature =>
            method.invoke(perceptron, feature.asInstanceOf[Vector]).asInstanceOf[Double]
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }

  }


  implicit class LocalLogisticRegression(val logisticRegression: LogisticRegressionModel) {

    import io.hydrosphere.mist.ml.DataUtils._

    def transform(localData: LocalData): LocalData = {
      logger.debug("Local LogisticRegression")
      logger.debug(localData.toString)
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
