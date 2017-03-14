package io.hydrosphere.mist.ml.classification

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTypedTransformer, Metadata}
import io.hydrosphere.mist.utils.DataUtils
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector}
import org.apache.spark.mllib.linalg.{DenseMatrix => OldDenseMatrix, DenseVector => OldDenseVector, Matrices => OldMatrices, SparseVector => SVector, Vector => OldVector, Vectors => OldVectors}

object LocalRandomForestClassificationModel extends LocalTypedTransformer[RandomForestClassificationModel] {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): RandomForestClassificationModel = {
    val treesMetadata = metadata.paramMap("treesMetadata").asInstanceOf[Map[String, Any]]
    val trees = treesMetadata map { treeKv =>
      val treeMeta = treeKv._2.asInstanceOf[Map[String,Any]]
      val meta = treeMeta("metadata").asInstanceOf[Metadata]
      LocalDecisionTreeClassificationModel.createTree(
        meta,
        data(treeKv._1).asInstanceOf[Map[String, Any]]
      )
    }

    val ctor = classOf[RandomForestClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[DecisionTreeClassificationModel]], classOf[Int], classOf[Int])
    ctor.setAccessible(true)
    ctor
      .newInstance(
        metadata.uid,
        trees.to[Array],
        metadata.numFeatures.get.asInstanceOf[java.lang.Integer],
        metadata.numClasses.get.asInstanceOf[java.lang.Integer]
      )
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
  }

  override def transformTyped(rndFrstClass: RandomForestClassificationModel, localData: LocalData): LocalData = {
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
