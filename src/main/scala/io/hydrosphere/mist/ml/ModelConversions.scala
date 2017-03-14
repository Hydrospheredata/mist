package io.hydrosphere.mist.ml

import io.hydrosphere.mist.ml.classification.{LocalDecisionTreeClassificationModel, LocalMultilayerPerceptronClassificationModel, LocalRandomForestClassificationModel}
import io.hydrosphere.mist.ml.clustering.{LocalGaussianMixtureModel, LocalKMeans}
import io.hydrosphere.mist.ml.preprocessors._
import io.hydrosphere.mist.ml.regression.{LocalDecisionTreeRegressionModel, LocalLogisticRegressionModel}
import org.apache.spark.ml.{PipelineModel, Transformer}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering.{GaussianMixtureModel, KMeansModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.DecisionTreeRegressionModel

import scala.language.implicitConversions

object ModelConversions {
  implicit def anyToLocal(m: Any): LocalModel = {
    println(m.getClass)
    m match {
      case _ : PipelineModel.type |_: PipelineModel  => LocalPipelineModel
      case _: MultilayerPerceptronClassificationModel.type | _: MultilayerPerceptronClassificationModel => LocalMultilayerPerceptronClassificationModel
      case _: Tokenizer.type | _: Tokenizer => LocalTokenizer
      case _: HashingTF.type | _: HashingTF => LocalHashingTF
      case _: StringIndexerModel.type | _: StringIndexerModel => LocalStringIndexer
      case _: LogisticRegressionModel.type | _: LogisticRegressionModel => LocalLogisticRegressionModel
      case _: DecisionTreeClassificationModel.type | _: DecisionTreeClassificationModel => LocalDecisionTreeClassificationModel
      case _: DecisionTreeRegressionModel.type | _: DecisionTreeRegressionModel => LocalDecisionTreeRegressionModel
      case _: RandomForestClassificationModel.type | _: RandomForestClassificationModel => LocalRandomForestClassificationModel
      case _: KMeansModel.type | _: KMeansModel => LocalKMeans
      case _: GaussianMixtureModel.type | _: GaussianMixtureModel => LocalGaussianMixtureModel
      case _: Binarizer.type | _: Binarizer => LocalBinarizer
      case _: PCAModel.type | _: PCAModel => LocalPCA
      case _: StandardScalerModel.type | _: StandardScalerModel => LocalStandardScaler
      case _: VectorIndexerModel.type | _: VectorIndexerModel => LocalVectorIndexer
      case _: MaxAbsScalerModel.type | _: MaxAbsScalerModel => LocalMaxAbsScaler
      case _: MinMaxScalerModel.type | _: MinMaxScalerModel => LocalMinMaxScaler
      case _: IndexToString.type | _: IndexToString => LocalIndexToString
      case _ => throw new Exception(s"Unknown model: ${m.getClass}")
    }
  }
}
