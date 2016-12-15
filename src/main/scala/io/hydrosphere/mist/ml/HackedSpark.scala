package io.hydrosphere.mist.ml

import org.apache.spark.ml.PredictionModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.Vector

import scala.reflect.{ClassTag, classTag}

object HackedSpark {

  implicit class HackedPipeline[FeaturesType: ClassTag, M <: PredictionModel[FeaturesType, M]](val predictor: PredictionModel[Vector, MultilayerPerceptronClassificationModel]) {
    def transform(features: Array[FeaturesType]): Array[Double] = {
      val method = predictor.getClass.getMethod("predict", classTag[FeaturesType].runtimeClass.asInstanceOf[Class[FeaturesType]])
      method.setAccessible(true)
      features map { (feature: FeaturesType) =>
        method.invoke(predictor, feature.asInstanceOf[Vector]).asInstanceOf[Double]
      }
    }
  }
}
