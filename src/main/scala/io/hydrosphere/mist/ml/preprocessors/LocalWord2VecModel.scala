package io.hydrosphere.mist.ml.preprocessors

import io.hydrosphere.mist.lib.{LocalData, LocalDataColumn}
import io.hydrosphere.mist.ml.{LocalModel, LocalTransformer, Metadata}
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.mllib.feature.{Word2VecModel => OldWord2VecModel}

import scala.reflect.runtime.universe

class LocalWord2VecModel(override val sparkTransformer: Word2VecModel) extends LocalTransformer[Word2VecModel] {
  lazy val parent: OldWord2VecModel = {
    val mirror = universe.runtimeMirror(sparkTransformer.getClass.getClassLoader)
    val parentTerm = universe.typeOf[Word2VecModel].decl(universe.TermName("wordVectors")).asTerm
    mirror.reflect(sparkTransformer).reflectField(parentTerm).get.asInstanceOf[OldWord2VecModel]
  }

  override def transform(localData: LocalData): LocalData = { // FIXME ?ML transform or old one?
    localData.column(sparkTransformer.getInputCol) match {
      case Some(column) =>
        val newColumn = LocalDataColumn(sparkTransformer.getOutputCol, column.data map { feature =>
          parent.transform(feature.toString)
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalWord2VecModel extends LocalModel[Word2VecModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): Word2VecModel = {
    import io.hydrosphere.mist.ml.DataUtils._

    val wordVectors = data("wordVectors").asInstanceOf[List[Float]].toArray
    val wordIndex: Map[String, Int] = data("wordIndex").asInstanceOf[Map[String, Any]].toScalaMap[String, Int]
    val oldCtor = classOf[OldWord2VecModel].getConstructor(classOf[Map[String, Int]], classOf[Array[Float]])
    oldCtor.setAccessible(true)
    val oldWord2VecModel = oldCtor.newInstance(wordIndex, wordVectors)

    val ctor = classOf[Word2VecModel].getConstructor(classOf[String], classOf[OldWord2VecModel])
    ctor.setAccessible(true)

    val inst = ctor.newInstance(metadata.uid, oldWord2VecModel)
      .setInputCol(metadata.paramMap("inputCol").toString)
      .setOutputCol(metadata.paramMap("outputCol").toString)

    inst
      .set(inst.maxIter, metadata.paramMap("maxIter").asInstanceOf[Int])
      .set(inst.seed, metadata.paramMap("seed").toString.toLong) // FIXME why seed is converted to int?
      .set(inst.numPartitions, metadata.paramMap("numPartitions").asInstanceOf[Int])
      .set(inst.stepSize, metadata.paramMap("stepSize").asInstanceOf[Double])
      .set(inst.maxSentenceLength, metadata.paramMap("maxSentenceLength").asInstanceOf[Int])
      .set(inst.windowSize, metadata.paramMap("windowSize").asInstanceOf[Int])
      .set(inst.vectorSize, metadata.paramMap("vectorSize").asInstanceOf[Int])
  }

  override implicit def getTransformer(transformer: Word2VecModel): LocalTransformer[Word2VecModel] = new LocalWord2VecModel(transformer)
}
