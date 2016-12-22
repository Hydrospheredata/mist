package io.hydrosphere.mist.ml.transformers

import io.hydrosphere.mist.ml.Metadata
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.classification.{LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}

import scala.language.implicitConversions
import scala.reflect.runtime.universe
import scala.collection.JavaConversions._

// TODO: better names 
trait HackedModel {
  def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer
}

object HackedPerceptron extends HackedModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): MultilayerPerceptronClassificationModel = {
    val constructor = classOf[MultilayerPerceptronClassificationModel].getDeclaredConstructor(classOf[String], classOf[Array[Int]], classOf[Vector])
    constructor.setAccessible(true)
    constructor.newInstance(metadata.uid, data("layers").asInstanceOf[List[Int]].to[Array], Vectors.dense(data("weights").asInstanceOf[Map[String, Any]]("values").asInstanceOf[List[Double]].toArray))
  }
}

object HackedTokenizer extends HackedModel {
  
  private case class Parameters(inputCol: String, outputCol: String)
  
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new Tokenizer(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
  }
}

object HackedHashingTF extends HackedModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    new HashingTF(metadata.uid)
      .setInputCol(metadata.paramMap("inputCol").asInstanceOf[String])
      .setOutputCol(metadata.paramMap("outputCol").asInstanceOf[String])
      .setBinary(metadata.paramMap("binary").asInstanceOf[Boolean])
      .setNumFeatures(metadata.paramMap("numFeatures").asInstanceOf[Int])
  }
}

object HackedLogisticRegressionModel extends HackedModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[LogisticRegressionModel].getDeclaredConstructor(classOf[String], classOf[Vector], classOf[Double])
    constructor.setAccessible(true)
    val coefficientsParams = data("coefficients").asInstanceOf[Map[String, Any]] 
    val coefficients = Vectors.sparse(
      coefficientsParams("size").asInstanceOf[Int],
      coefficientsParams("indices").asInstanceOf[List[Int]].toArray[Int],
      coefficientsParams("values").asInstanceOf[List[Double]].toArray[Double]
    )
    constructor
      .newInstance(metadata.uid, coefficients, data("intercept").asInstanceOf[java.lang.Double])
      .setFeaturesCol(metadata.paramMap("featuresCol").asInstanceOf[String])
      .setPredictionCol(metadata.paramMap("predictionCol").asInstanceOf[String])
      .setProbabilityCol(metadata.paramMap("probabilityCol").asInstanceOf[String])
      .setThreshold(metadata.paramMap("threshold").asInstanceOf[Double])
  }
}

object HackedPipelineModel extends HackedModel {
  override def localLoad(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val constructor = classOf[PipelineModel].getDeclaredConstructor(classOf[String], classOf[java.util.List[Transformer]])
    constructor.setAccessible(true)
    val stages: java.util.List[Transformer] = data("stages").asInstanceOf[List[Transformer]] 
    constructor.newInstance(metadata.uid, stages)
  }
}

object ModelConversions {
  implicit def anyToLocal(m: Any): HackedModel = m match {
    case _: MultilayerPerceptronClassificationModel.type => HackedPerceptron
    case _: Tokenizer.type => HackedTokenizer
    case _: HashingTF.type  => HackedHashingTF
    case _: LogisticRegressionModel.type  => HackedLogisticRegressionModel
    case _: PipelineModel.type  => HackedPipelineModel
    case _ => throw new Exception(s"Unknown transformer: ${m.getClass}")
  }
}

object TransformerFactory {

  import ModelConversions._
  
  def apply(metadata: Metadata, data: Map[String, Any]): Transformer = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(metadata.className + "$")
    val obj = runtimeMirror.reflectModule(module)
    val localModel: HackedModel = obj.instance
    localModel.localLoad(metadata, data)
  }
  
}
