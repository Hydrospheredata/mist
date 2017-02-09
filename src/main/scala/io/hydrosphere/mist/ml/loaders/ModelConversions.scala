package io.hydrosphere.mist.ml.loaders

import io.hydrosphere.mist.ml.loaders.classification.{LocalDecisionTreeClassificationModel, LocalPerceptron, LocalRandomForestClassificationModel}
import io.hydrosphere.mist.ml.loaders.clustering.LocalKMeans
import io.hydrosphere.mist.ml.loaders.preprocessors.{LocalHashingTF, LocalTokenizer}
import io.hydrosphere.mist.ml.loaders.regression.LocalLogisticRegressionModel
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}

import scala.language.implicitConversions

object ModelConversions {
  implicit def anyToLocal(m: Any): LocalModel = m match {
    case _: MultilayerPerceptronClassificationModel.type => LocalPerceptron
    case _: Tokenizer.type => LocalTokenizer
    case _: HashingTF.type  => LocalHashingTF
    case _: LogisticRegressionModel.type  => LocalLogisticRegressionModel
    case _: PipelineModel.type  => LocalPipelineModel
    case _: DecisionTreeClassificationModel.type  => LocalDecisionTreeClassificationModel
    case _: RandomForestClassificationModel.type => LocalRandomForestClassificationModel
    case _: KMeansModel.type => LocalKMeans
    case _ => throw new Exception(s"Unknown transformer: ${m.getClass}")
  }
}
