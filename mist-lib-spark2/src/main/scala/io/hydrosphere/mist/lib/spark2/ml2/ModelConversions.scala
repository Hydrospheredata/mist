package io.hydrosphere.mist.lib.spark2.ml2

import io.hydrosphere.mist.lib.spark2.ml2.classification._
import io.hydrosphere.mist.lib.spark2.ml2.clustering._
import io.hydrosphere.mist.lib.spark2.ml2.preprocessors._
import io.hydrosphere.mist.lib.spark2.ml2.regression._

import org.apache.spark.ml.classification._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression._

import org.apache.spark.ml.{PipelineModel, Transformer}

import scala.language.implicitConversions

object ModelConversions {
  implicit def sparkToLocal[T <: Transformer](m: Any): LocalModel[T] = {
    m match {
      case _ : PipelineModel.type  => LocalPipelineModel

      case x: LocalModel[T] => x

      // Classification models
      case _: DecisionTreeClassificationModel.type  => LocalDecisionTreeClassificationModel
      case _: MultilayerPerceptronClassificationModel.type => LocalMultilayerPerceptronClassificationModel
      case _: NaiveBayes.type => LocalNaiveBayes
      case _: RandomForestClassificationModel.type => LocalRandomForestClassificationModel

        // Clustering models
      case _: BisectingKMeansModel.type => LocalBisectingKMeansModel
      case _: GaussianMixtureModel.type => LocalGaussianMixtureModel
      case _: KMeansModel.type  => LocalKMeansModel

        // Preprocessing
      case _: Binarizer.type => LocalBinarizer
      case _: Bucketizer.type => LocalBucketizer
      case _: DCT.type => LocalDCT
      case _: ElementwiseProduct.type => LocalElementwiseProduct
      case _: HashingTF.type => LocalHashingTF
      case _: IndexToString.type => LocalIndexToString
      case _: Interaction.type => LocalInteraction
      case _: MaxAbsScalerModel.type => LocalMaxAbsScalerModel
      case _: MinMaxScalerModel.type => LocalMinMaxScalerModel
      case _: NGram.type => LocalNGram
      case _: Normalizer.type => LocalNormalizer
      case _: OneHotEncoder.type => LocalOneHotEncoder
      case _: PCAModel.type => LocalPCAModel
      case _: PolynomialExpansion.type => LocalPolynomialExpansion
      case _: SQLTransformer.type => LocalSQLTransformer
      case _: StandardScalerModel.type => LocalStandardScalerModel
      case _: StopWordsRemover.type  => LocalStopWordsRemover
      case _: StringIndexerModel.type => LocalStringIndexerModel
      case _: Tokenizer.type  => LocalTokenizer
      case _: VectorAssembler.type => LocalVectorAssembler
      case _: VectorIndexerModel.type => LocalVectorIndexerModel
      case _: VectorSlicer.type => LocalVectorSlicer

        // Regression
      case _: LogisticRegressionModel.type => LocalLogisticRegressionModel
      case _: DecisionTreeRegressionModel.type => LocalDecisionTreeRegressionModel

      case _ => throw new Exception(s"Unknown model: ${m.getClass}")
    }
  }
}
