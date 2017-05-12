package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.MaxAbsScalerModel
import org.apache.spark.ml.linalg.{DenseVector, Vector, Vectors, SparseVector}

class LocalMaxAbsScalerModel(override val sparkTransformer: MaxAbsScalerModel) extends LocalTransformer[MaxAbsScalerModel] {
  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val maxAbsUnzero = Vectors.dense(sparkTransformer.maxAbs.toArray.map(x => if (x == 0) 1 else x))
        val newData = column.data.map(r => {
          val vec: List[Double] = r match {
            case d: SparseVector => d.toDense.toArray.toList
            case d: DenseVector => d.toArray.toList
            case d: List[Any @unchecked] => d map (_.toString.toDouble)
            case d => throw new IllegalArgumentException(s"Unknown data type for LocalMaxAbsScaler: $d")
          }
          val brz = DataUtils.asBreeze(vec.toArray) / DataUtils.asBreeze(maxAbsUnzero.toArray)
          DataUtils.fromBreeze(brz)
        })
        localData.withColumn(LocalDataColumn(sparkTransformer.getOutputCol, newData))
      case None => localData
    }
  }
}

object LocalMaxAbsScalerModel extends LocalModel[MaxAbsScalerModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): MaxAbsScalerModel = {
    val maxAbsList = data("maxAbs").
      asInstanceOf[Map[String, Any]].
      getOrElse("values", List()).
      asInstanceOf[List[Double]].toArray
    val maxAbs = new DenseVector(maxAbsList)

    val constructor = classOf[MaxAbsScalerModel].getDeclaredConstructor(classOf[String], classOf[Vector])
    constructor.setAccessible(true)
    constructor
      .newInstance(metadata.uid, maxAbs)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }

  override implicit def getTransformer(transformer: MaxAbsScalerModel): LocalTransformer[MaxAbsScalerModel] = new LocalMaxAbsScalerModel(transformer)
}
