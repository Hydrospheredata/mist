package io.hydrosphere.mist.ml.loaders

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{LogisticRegressionModel, MultilayerPerceptronClassificationModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

import scala.language.implicitConversions

object ModelConversions {
  implicit def anyToLocal(m: Any): LocalModel = m match {
    case _: MultilayerPerceptronClassificationModel.type => LocalPerceptron
    case _: Tokenizer.type => LocalTokenizer
    case _: HashingTF.type  => LocalHashingTF
    case _: LogisticRegressionModel.type  => LocalLogisticRegressionModel
    case _: PipelineModel.type  => LocalPipelineModel
    case _ => throw new Exception(s"Unknown transformer: ${m.getClass}")
  }
}
