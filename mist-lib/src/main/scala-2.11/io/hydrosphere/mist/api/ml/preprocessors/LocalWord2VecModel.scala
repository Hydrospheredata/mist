package io.hydrosphere.mist.api.ml.preprocessors

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.ml.linalg.{DenseVector, Vectors}
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}

class LocalWord2VecModel(override val sparkTransformer: Word2VecModel) extends LocalTransformer[Word2VecModel] {
  lazy val parent: OldWord2VecModel = {
    val field = sparkTransformer.getClass.getDeclaredField("org$apache$spark$ml$feature$Word2VecModel$$wordVectors")
    field.setAccessible(true)
    field.get(sparkTransformer).asInstanceOf[OldWord2VecModel]
  }

  private def axpy(a: Double, x: Array[Double], y: Array[Double]) = {
    y.zipWithIndex.foreach {
      case (value, index) =>
        y.update(index, x(index)*a + value)
    }
  }

  private def scal(a: Double, v: Array[Double]) = {
    v.zipWithIndex.foreach{
      case (value, index) =>
        v.update(index, value * a)
    }
  }

  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val data = if (column.data.isEmpty) {
          List(Array.fill(sparkTransformer.getVectorSize){0.0})
        } else {
          val vectors = parent.getVectors
            .mapValues(v => Vectors.dense(v.map(_.toDouble)))
          val sum = Array.fill(sparkTransformer.getVectorSize){0.0}
          column.data.map(_.asInstanceOf[String]).foreach { word =>
            vectors.get(word).foreach { vec =>
              axpy(1.0, vec.toDense.values, sum)
            }
          }
          scal(1.0 / column.data.length, sum)
          List(sum)
        }
        val newColumn = LocalDataColumn(sparkTransformer.getOutputCol, data)
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalWord2VecModel extends LocalModel[Word2VecModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): Word2VecModel = {
    val wordVectors = data("wordVectors").asInstanceOf[List[Float]].toArray
    val wordIndex = data("wordIndex").asInstanceOf[Map[String, Int]]
    val oldCtor = classOf[OldWord2VecModel].getConstructor(classOf[Map[String, Int]], classOf[Array[Float]])
    oldCtor.setAccessible(true)

    val oldWord2VecModel = oldCtor.newInstance(wordIndex, wordVectors)

    val ctor = classOf[Word2VecModel].getConstructor(classOf[String], classOf[OldWord2VecModel])
    ctor.setAccessible(true)

    val inst = ctor.newInstance(metadata.uid, oldWord2VecModel)
      .setInputCol(metadata.paramMap("inputCol").toString)
      .setOutputCol(metadata.paramMap("outputCol").toString)

    inst
      .set(inst.maxIter, metadata.paramMap("maxIter").asInstanceOf[Number].intValue())
      .set(inst.seed, metadata.paramMap("seed").toString.toLong) // FIXME why seed is converted to int?
      .set(inst.numPartitions, metadata.paramMap("numPartitions").asInstanceOf[Number].intValue())
      .set(inst.stepSize, metadata.paramMap("stepSize").asInstanceOf[Double])
      .set(inst.maxSentenceLength, metadata.paramMap("maxSentenceLength").asInstanceOf[Number].intValue())
      .set(inst.windowSize, metadata.paramMap("windowSize").asInstanceOf[Number].intValue())
      .set(inst.vectorSize, metadata.paramMap("vectorSize").asInstanceOf[Number].intValue())
  }

  override implicit def getTransformer(transformer: Word2VecModel): LocalTransformer[Word2VecModel] = new LocalWord2VecModel(transformer)
}
