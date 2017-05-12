package io.hydrosphere.mist.api.ml.classification

import java.lang.reflect.InvocationTargetException

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{DenseVector, Matrix, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.{DoubleParam, Param, ParamValidators}
import org.apache.spark.mllib.linalg.{SparseVector => OldSparseVector}

class LocalNaiveBayes(override val sparkTransformer: NaiveBayesModel) extends LocalTransformer[NaiveBayesModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val cls = classOf[NaiveBayesModel]
        val rawPredictionCol = LocalDataColumn(sparkTransformer.getRawPredictionCol, column.data map { data =>
          val vector = data match {
            case v: SparseVector => v
            case v: OldSparseVector => DataUtils.mllibVectorToMlVector(v)
            case r: List[Any] =>
              val v = r.map(_.toString).map(_.toDouble).toArray
              Vectors.dense(v)
            case x => throw new IllegalArgumentException(s"$x is not a vector")
          }
          val predictRaw = cls.getDeclaredMethod("predictRaw", classOf[Vector])
          predictRaw.invoke(sparkTransformer, vector)
        })
        val probabilityCol = LocalDataColumn(sparkTransformer.getProbabilityCol, rawPredictionCol.data map { data =>
          val vector = data match {
            case v: DenseVector => v
            case x => throw new IllegalArgumentException(s"$x is not a dense vector")
          }
          val raw2probabilityInPlace = cls.getDeclaredMethod("raw2probabilityInPlace", classOf[Vector])
          raw2probabilityInPlace.invoke(sparkTransformer, vector.copy)
        })
        val predictionCol = LocalDataColumn(sparkTransformer.getPredictionCol, rawPredictionCol.data map { data =>
          val vector = data match {
            case v: DenseVector => v
            case x => throw new IllegalArgumentException(s"$x is not a dense vector")
          }
          val raw2prediction = cls.getMethod("raw2prediction", classOf[Vector])
          raw2prediction.invoke(sparkTransformer, vector.copy)
        })
        localData.withColumn(rawPredictionCol)
          .withColumn(probabilityCol)
          .withColumn(predictionCol)
      case None => localData
    }
  }
}

object LocalNaiveBayes extends LocalModel[NaiveBayesModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): NaiveBayesModel = {
    val constructor = classOf[NaiveBayesModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Matrix])
    constructor.setAccessible(true)

    val matrixMetadata = data("theta").asInstanceOf[Map[String, Any]]
    val matrix = DataUtils.constructMatrix(matrixMetadata)
    val pi = data("pi").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val piVec = Vectors.dense(pi)

    val nb = constructor
      .newInstance(metadata.uid, piVec, matrix)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setRawPredictionCol(metadata.paramMap("rawPredictionCol").asInstanceOf[String])

    nb.set(nb.smoothing, metadata.paramMap("smoothing").asInstanceOf[Number].doubleValue())
    nb.set(nb.modelType, metadata.paramMap("modelType").asInstanceOf[String])
    nb.set(nb.labelCol, metadata.paramMap("labelCol").asInstanceOf[String])

    nb
  }

  override implicit def getTransformer(transformer: NaiveBayesModel): LocalTransformer[NaiveBayesModel] = new LocalNaiveBayes(transformer)
}
