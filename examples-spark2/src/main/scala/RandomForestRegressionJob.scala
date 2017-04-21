import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.regression.RandomForestRegressor


object RandomForestRegressionJob extends MLMistJob with SQLSupport {
  def train(savePath: String, datasetPath: String): Map[String, Any] = {
    // Load and parse the data file, converting it to a DataFrame.
    val data = session.read.format("libsvm").load(datasetPath)

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(trainingData)


    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[List[Double]]): Map[String, Any] = {
    import LocalPipelineModel._
    import io.hydrosphere.mist.lib.spark2.ml.DataUtils._

    val pipeline = PipelineLoader.load(modelPath)
    val arrays = features.map(_.forceDoubles.toArray).map(Vectors.dense)
    val data = LocalData(
      LocalDataColumn("features", arrays)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.toMapList)
  }
}
