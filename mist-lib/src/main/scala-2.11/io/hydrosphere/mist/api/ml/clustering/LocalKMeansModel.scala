package io.hydrosphere.mist.api.ml.clustering

import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.mllib.clustering.{KMeansModel => OldKMeansModel}
import org.apache.spark.mllib.clustering.{KMeansModel => MLlibKMeans}
import org.apache.spark.mllib.linalg.{Vectors, Vector => MLlibVec}

import scala.collection.immutable.ListMap
import scala.reflect.runtime.universe

class LocalKMeansModel(override val sparkTransformer: KMeansModel) extends LocalTransformer[KMeansModel] {
  lazy val parent: OldKMeansModel = {
    val mirror = universe.runtimeMirror(sparkTransformer.getClass.getClassLoader)
    val parentTerm = universe.typeOf[KMeansModel].decl(universe.TermName("parentModel")).asTerm
    mirror.reflect(sparkTransformer).reflectField(parentTerm).get.asInstanceOf[OldKMeansModel]
  }

  override def transform(localData: LocalData): LocalData = {
    localData.column(sparkTransformer.getFeaturesCol) match {
      case Some(column) =>
        val newColumn = LocalDataColumn(sparkTransformer.getPredictionCol, column.data.map(f => Vectors.dense(f.asInstanceOf[Array[Double]])).map { vector =>
          parent.predict(vector)
        })
        localData.withColumn(newColumn)
      case None => localData
    }
  }
}

object LocalKMeansModel extends LocalModel[KMeansModel] {
  override def load(metadata: Metadata, data: Map[String, Any]): KMeansModel = {
    val sorted = ListMap(data.toSeq.sortBy { case (key: String, _: Any) => key.toInt}: _*)
    val centers = sorted map {
      case (_: String, value: Any) =>
        val center = value.asInstanceOf[Map[String, Any]]
        Vectors.dense(center("values").asInstanceOf[List[Double]].to[Array])
    }
    val parentConstructor = classOf[MLlibKMeans].getDeclaredConstructor(classOf[Array[MLlibVec]])
    parentConstructor.setAccessible(true)
    val mlk = parentConstructor.newInstance(centers.toArray)

    val constructor = classOf[KMeansModel].getDeclaredConstructor(classOf[String], classOf[MLlibKMeans])
    constructor.setAccessible(true)
    var inst = constructor
      .newInstance(metadata.uid, mlk)
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])

    inst = inst.set(inst.k, metadata.paramMap("k").asInstanceOf[Number].intValue())
    inst = inst.set(inst.initMode, metadata.paramMap("initMode").asInstanceOf[String])
    inst = inst.set(inst.maxIter, metadata.paramMap("maxIter").asInstanceOf[Number].intValue())
    inst = inst.set(inst.initSteps, metadata.paramMap("initSteps").asInstanceOf[Number].intValue())
    inst = inst.set(inst.seed, metadata.paramMap("seed").toString.toLong)
    inst = inst.set(inst.tol, metadata.paramMap("tol").asInstanceOf[Double])
    inst
  }
  override implicit def getTransformer(transformer: KMeansModel): LocalTransformer[KMeansModel] = new LocalKMeansModel(transformer)
}
