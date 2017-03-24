package io.hydrosphere.mist.lib.spark2.ml

import io.hydrosphere.mist.lib.spark2.{LocalData, LocalDataColumn}
import org.apache.spark.SparkException
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, LogisticRegressionModel, MultilayerPerceptronClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.mllib.feature.{HashingTF => HTF, StandardScalerModel => OldStandardScalerModel}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

import scala.collection.mutable
import scala.language.implicitConversions

object LocalTransformers {

  implicit class LocalPipeline(val pipeline: PipelineModel) {
    def transform(localData: LocalData): LocalData = {
      pipeline.stages.foldLeft(localData)((x: LocalData, y: Transformer) => y match {
        case tokenizer: Tokenizer => tokenizer.transform(x)
        case hashingTF: HashingTF => hashingTF.transform(x)
        case strIndexer: StringIndexerModel => strIndexer.transform(x)
        case logisticRegression: LogisticRegressionModel => logisticRegression.transform(x)
        case perceptron: MultilayerPerceptronClassificationModel => perceptron.transform(x)
        case classTree: DecisionTreeClassificationModel => classTree.transform(x)
        case regTree: DecisionTreeRegressionModel => regTree.transform(x)
        case gaussianModel: GaussianMixtureModel => gaussianModel.transform(x)
        case binarizer: Binarizer => binarizer.transform(x)
        case pca: PCAModel => pca.transform(x)
        case standardScaler: StandardScalerModel => standardScaler.transform(x)
        case minMaxScaler: MinMaxScalerModel => minMaxScaler.transform(x)
        case maxAbsScaler: MaxAbsScalerModel => maxAbsScaler.transform(x)
        case vecIndx: VectorIndexerModel => vecIndx.transform(x)
        case indxStr: IndexToString => indxStr.transform(x)
        case rndFrstClass: RandomForestClassificationModel => rndFrstClass.transform(x)
        case _ => throw new Exception(s"Unknown pipeline stage: ${y.getClass}")
      })
    }
  }

  implicit class LocalRandomForestClassificationModel(val rndFrstClass: RandomForestClassificationModel) {
    def transform(localData: LocalData): LocalData = {
      localData.column(rndFrstClass.getFeaturesCol) match {
        case Some(column) =>
          val cls = rndFrstClass.getClass
          val rawPredictionCol = LocalDataColumn(rndFrstClass.getRawPredictionCol, column.data map { data =>
            val vector = data match {
              case v: SparseVector => v
              case v: SVector => DataUtils.mllibVectorToMlVector(v)
              case x => throw new IllegalArgumentException(s"$x is not a vector")
            }
            val predictRaw = cls.getDeclaredMethod("predictRaw", classOf[Vector])
            predictRaw.invoke(rndFrstClass, vector)
          })
          val probabilityCol = LocalDataColumn(rndFrstClass.getProbabilityCol, rawPredictionCol.data map { data =>
            val vector = data match {
              case v: DenseVector => v
              case x => throw new IllegalArgumentException(s"$x is not a dense vector")
            }
            val raw2probabilityInPlace = cls.getDeclaredMethod("raw2probabilityInPlace", classOf[Vector])
            raw2probabilityInPlace.invoke(rndFrstClass, vector.copy)
          })
          val predictionCol = LocalDataColumn(rndFrstClass.getPredictionCol, rawPredictionCol.data map { data =>
            val vector = data match {
              case v: DenseVector => v
              case x => throw new IllegalArgumentException(s"$x is not a dense vector")
            }
            val raw2prediction = cls.getMethod("raw2prediction", classOf[Vector])
            raw2prediction.invoke(rndFrstClass, vector.copy)
          })
          localData.withColumn(rawPredictionCol)
            .withColumn(probabilityCol)
            .withColumn(predictionCol)
        case None => localData
      }
    }
  }

  implicit class LocalIndexToString(val indxStr: IndexToString) {
    def transform(localData: LocalData): LocalData = {
      localData.column(indxStr.getInputCol) match {
        case Some(column) =>
          val labels = indxStr.getLabels
          val indexer = (index: Double) => {
            val idx = index.toInt
            if (0 <= idx && idx < labels.length) {
              labels(idx)
            } else {
              throw new SparkException(s"Unseen index: $index ??")
            }
          }
          val newColumn = LocalDataColumn(indxStr.getOutputCol, column.data map {
              case d: Double => indexer(d)
              case d => throw new IllegalArgumentException(s"Unknown data to index: $d")
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }

  implicit class LocalVectorIndexerModel(val vecIndx: VectorIndexerModel) {
    def transform(localData: LocalData): LocalData = {
      localData.column(vecIndx.getInputCol) match {
        case Some(column) =>
          val newColumn = LocalDataColumn(vecIndx.getOutputCol, column.data map { data =>
            val vec = data.asInstanceOf[Vector]
            transformFunc(vec)
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
    private val transformFunc: Vector => Vector = {
      val sortedCatFeatureIndices = vecIndx.categoryMaps.keys.toArray.sorted
      val localVectorMap = vecIndx.categoryMaps
      val localNumFeatures = vecIndx.numFeatures
      val f: Vector => Vector = { (v: Vector) =>
        assert(v.size == localNumFeatures, "VectorIndexerModel expected vector of length" +
          s" $vecIndx.numFeatures but found length ${v.size}")
        v match {
          case dv: DenseVector =>
            val tmpv = dv.copy
            localVectorMap.foreach { case (featureIndex: Int, categoryMap: Map[Double, Int]) =>
              tmpv.values(featureIndex) = categoryMap(tmpv(featureIndex))
            }
            tmpv
          case sv: SparseVector =>
            // We use the fact that categorical value 0 is always mapped to index 0.
            val tmpv = sv.copy
            var catFeatureIdx = 0 // index into sortedCatFeatureIndices
          var k = 0 // index into non-zero elements of sparse vector
            while (catFeatureIdx < sortedCatFeatureIndices.length && k < tmpv.indices.length) {
              val featureIndex = sortedCatFeatureIndices(catFeatureIdx)
              if (featureIndex < tmpv.indices(k)) {
                catFeatureIdx += 1
              } else if (featureIndex > tmpv.indices(k)) {
                k += 1
              } else {
                tmpv.values(k) = localVectorMap(featureIndex)(tmpv.values(k))
                catFeatureIdx += 1
                k += 1
              }
            }
            tmpv
        }
      }
      f
    }
  }

  implicit class LocalDecisionTreeRegressionModel(val tree: DecisionTreeRegressionModel) {

    def transform(localData: LocalData): LocalData = {
      localData.column(tree.getFeaturesCol) match {
        case Some(column) =>
          val method = classOf[DecisionTreeRegressionModel].getMethod("predict", classOf[Vector])
          method.setAccessible(true)
          val newColumn = LocalDataColumn(tree.getPredictionCol, column.data map { feature =>
            val vector: SparseVector = feature match {
              case v: SparseVector => v
              case v: SVector => DataUtils.mllibVectorToMlVector(v)
              case x => throw new IllegalArgumentException(s"$x is not a vector")
            }
            method.invoke(tree, vector).asInstanceOf[Double]
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }

  implicit class LocalDecisionTreeClassificationModel(val tree: DecisionTreeClassificationModel) {

    def transform(localData: LocalData): LocalData = {
      localData.column(tree.getFeaturesCol) match {
        case Some(column) =>
          val method = classOf[DecisionTreeClassificationModel].getMethod("predict", classOf[Vector])
          method.setAccessible(true)
          val newColumn = LocalDataColumn(tree.getPredictionCol, column.data map { feature =>
            val vector: SparseVector = feature match {
              case v: SparseVector => v
              case v: SVector => DataUtils.mllibVectorToMlVector(v)
              case x => throw new IllegalArgumentException(s"$x is not a vector")
            }
            method.invoke(tree, vector).asInstanceOf[Double]
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }
  
  implicit class LocalTokenizer(val tokenizer: Tokenizer) {
    def transform(localData: LocalData): LocalData = {
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
      localData.column(strIndexer.getInputCol) match {
        case Some(column) =>
          val labelToIndex = {
            val n = strIndexer.labels.length
            val map = new mutable.HashMap[String, Double]
            var i = 0
            while (i < n) {
              map.update(strIndexer.labels(i), i)
              i += 1
            }
            map
          }
          val indexer = (label: String) => {
            if (labelToIndex.contains(label)) {
              labelToIndex(label)
            } else {
              throw new SparkException(s"Unseen label: $label.")
            }
          }
          val newColumn = LocalDataColumn(strIndexer.getOutputCol, column.data map { feature =>
            val str = feature.asInstanceOf[String]
            indexer(str)
          })
          localData.withColumn(newColumn)
        case None => localData
      }
    }
  }

  implicit class LocalMultilayerPerceptronClassificationModel(val perceptron: MultilayerPerceptronClassificationModel) {

    def transform(localData: LocalData): LocalData = {
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

    import DataUtils._

    def transform(localData: LocalData): LocalData = {
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
      localData.column(pca.getInputCol) match {
        case Some(column) =>
          val newData = column.data.map(r => {
            val pc = OldMatrices.fromML(pca.pc).asInstanceOf[OldDenseMatrix]
            val vec: List[Double] = r match {
              case d: List[Any @unchecked] =>
                val l: List[Double] = d map (_.toString.toDouble)
                l
              case d => throw new IllegalArgumentException(s"Unknown data type for LocalPCA: $d")
            }
            val vector = OldVectors.dense(vec.toArray)
            pc.transpose.multiply(vector)
          })
          localData.withColumn(LocalDataColumn(pca.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalStandardScaler(val standardScaler: StandardScalerModel) {
    def transform(localData: LocalData): LocalData = {
      localData.column(standardScaler.getInputCol) match {
        case Some(column) =>
          val scaler = new OldStandardScalerModel(
            OldVectors.fromML(standardScaler.std.asInstanceOf[Vector]),
            OldVectors.fromML(standardScaler.mean.asInstanceOf[Vector]),
            standardScaler.getWithStd,
            standardScaler.getWithMean
          )

          val newData = column.data.map(r => {
            val vec: List[Double] = r match {
              case d: List[Any @unchecked] =>
                val l: List[Double] = d map (_.toString.toDouble)
                l
              case d => throw new IllegalArgumentException(s"Unknown data type for LocalStandardScaler: $d")
            }
            val vector: OldVector = OldVectors.dense(vec.toArray)
            scaler.transform(vector)
          })
          localData.withColumn(LocalDataColumn(standardScaler.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalMaxAbsScaler(val maxAbsScaler: MaxAbsScalerModel) {
    def transform(localData: LocalData): LocalData = {
      localData.column(maxAbsScaler.getInputCol) match {
        case Some(column) =>
          val maxAbsUnzero = Vectors.dense(maxAbsScaler.maxAbs.toArray.map(x => if (x == 0) 1 else x))
          val newData = column.data.map(r => {
            val vec: List[Double] = r match {
              case d: List[Any @unchecked] =>
                val l: List[Double] = d map (_.toString.toDouble)
                l
              case d => throw new IllegalArgumentException(s"Unknown data type for LocalMaxAbsScaler: $d")
            }
            val brz = DataUtils.asBreeze(vec.toArray) / DataUtils.asBreeze(maxAbsUnzero.toArray)
            DataUtils.fromBreeze(brz)
          })
          localData.withColumn(LocalDataColumn(maxAbsScaler.getOutputCol, newData))
        case None => localData
      }
    }
  }

  implicit class LocalMinMaxScaler(val minMaxScaler: MinMaxScalerModel) {
    def transform(localData: LocalData): LocalData = {
      val originalRange = (DataUtils.asBreeze(minMaxScaler.originalMax.toArray) - DataUtils.asBreeze(minMaxScaler.originalMin.toArray)).toArray
      val minArray = minMaxScaler.originalMin.toArray
      val min = minMaxScaler.getMin
      val max = minMaxScaler.getMax

      localData.column(minMaxScaler.getInputCol) match {
        case Some(column) =>
          val newData = column.data.map(r => {
            val scale = max - min
            val vec: List[Double] = r match {
              case d: List[Any] =>
                val l: List[Double] = d map (_.toString.toDouble)
                l
              case d => throw new IllegalArgumentException(s"Unknown data type for LocalMinMaxScaler: $d")
            }
            val values = vec.toArray
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
