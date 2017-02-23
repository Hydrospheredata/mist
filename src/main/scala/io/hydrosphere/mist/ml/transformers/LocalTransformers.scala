package io.hydrosphere.mist.ml.transformers

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.loaders.preprocessors.{LocalMaxAbsScaler, LocalStandardScaler}
import io.hydrosphere.mist.utils.Logger
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.mllib.feature.{HashingTF => HTF, StandardScalerModel => OldStandardScalerModel}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}
import org.apache.spark.sql.{DataFrame, Dataset}
import io.hydrosphere.mist.ml.DataUtils

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
        case gaussianModel: GaussianMixtureModel => gaussianModel.transform(x)
        case binarizer: Binarizer => binarizer.transform(x)
        case pca: PCAModel => pca.transform(x)
        case standardScaler: StandardScalerModel => standardScaler.transform(x)
        case minMaxScaler: MinMaxScalerModel => minMaxScaler.transform(x)
        case maxAbsScaler: MaxAbsScalerModel => maxAbsScaler.transform(x)
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
            ???
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

  // TODO: test
  implicit class LocalGaussianMixtureModel(val gaussianModel: GaussianMixtureModel) {
    def transform(localData: LocalData): LocalData = {
      logger.debug("Local GaussianMixture")
      logger.debug(localData.toString)
      localData.column(gaussianModel.getFeaturesCol) match {
        case Some(column) =>
          val predictMethod = classOf[GaussianMixtureModel].getMethod("predict", classOf[Vector])
          predictMethod.setAccessible(true)
          val newColumn = LocalDataColumn(gaussianModel.getPredictionCol, column.data map { feature =>
            predictMethod.invoke(gaussianModel, feature.asInstanceOf[Vector]).asInstanceOf[Int]
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }

  implicit class LocalBinarizer(val binarizer: Binarizer) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local Binarizer")
      logger.debug(localData.toString)
      localData.column(binarizer.getInputCol) match {
        case Some(column) =>
          val trashhold: Double = binarizer.getThreshold
          val newData = column.data.map(r => {
            if (r.asInstanceOf[Double] > trashhold) 1.0 else 0.0
          })
          localData.withColumn(LocalDataColumn(binarizer.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalPCA(val pca: PCAModel) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local PCA")
      logger.debug(localData.toString)

      println(localData.toString)

      localData.column(pca.getInputCol) match {
        case Some(column) =>
          val newData = column.data.map(r => {
            val pc = OldMatrices.fromML(pca.pc).asInstanceOf[OldDenseMatrix]
            val vector = OldVectors.dense(r.asInstanceOf[Array[Double]])
            pc.transpose.multiply(vector)
          })
          localData.withColumn(LocalDataColumn(pca.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalStandardScaler(val standardScaler: StandardScalerModel) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local StandardScaler")
      logger.debug(localData.toString)
      localData.column(standardScaler.getInputCol) match {
        case Some(column) =>
          val scaler = new OldStandardScalerModel(
            OldVectors.fromML(standardScaler.std.asInstanceOf[Vector]),
            OldVectors.fromML(standardScaler.mean.asInstanceOf[Vector]),
            standardScaler.getWithStd,
            standardScaler.getWithMean
          )

          val newData = column.data.map(r => {
            val vector: OldVector = OldVectors.dense(r.asInstanceOf[Array[Double]])
            scaler.transform(vector)
          })
          localData.withColumn(LocalDataColumn(standardScaler.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalMaxAbsScaler(val maxAbsScaler: MaxAbsScalerModel) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local MaxAbsScaler")
      logger.debug(localData.toString)
      localData.column(maxAbsScaler.getInputCol) match {
        case Some(column) =>
          val maxAbsUnzero = Vectors.dense(maxAbsScaler.maxAbs.toArray.map(x => if (x == 0) 1 else x))
          val newData = column.data.map(r => {
            val brz = DataUtils.asBreeze(r.asInstanceOf[Array[Double]]) / DataUtils.asBreeze(maxAbsUnzero.toArray)
            DataUtils.fromBreeze(brz)
          })
          localData.withColumn(LocalDataColumn(maxAbsScaler.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalMinMaxScaler(val minMaxScaler: MinMaxScalerModel) {
    def transform(localData: LocalData): LocalData = {
      logger.debug(s"Local MinMaxScaler")
      logger.debug(localData.toString)

      val originalRange = (DataUtils.asBreeze(minMaxScaler.originalMax.toArray) - DataUtils.asBreeze(minMaxScaler.originalMin.toArray)).toArray
      val minArray = minMaxScaler.originalMin.toArray
      val min = minMaxScaler.getMin
      val max = minMaxScaler.getMax

      localData.column(minMaxScaler.getInputCol) match {
        case Some(column) =>

          val newData = column.data.map(r => {
            val scale = max - min

            val values = r.asInstanceOf[Array[Double]]
            val size = values.length
            var i = 0
            while (i < size) {
              if (!values(i).isNaN) {
                val raw = if (originalRange(i) != 0) (values(i) - minArray(i)) / originalRange(i) else 0.5
                values(i) = raw * scale + min
              }
              i += 1
            }
            values
          })
          localData.withColumn(LocalDataColumn(minMaxScaler.getOutputCol, newData))
        case None => localData
      }
    }
  }


}
