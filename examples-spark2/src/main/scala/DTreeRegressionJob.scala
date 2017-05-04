import io.hydrosphere.mist.api.ml._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressor
import org.apache.spark.ml.linalg.{Vector, Vectors}

object DTreeRegressionJob extends MLMistJob with SQLSupport {
  def constructVector(params: Map[String, Any]): Vector = {
    Vectors.sparse(
      params("size").asInstanceOf[Int],
      params("indices").asInstanceOf[List[Int]].toArray[Int],
      params("values").asInstanceOf[List[Int]].map(_.toDouble).toArray[Double]
    )
  }

  def train(datasetPath: String, savePath: String): Map[String, Any] = {
    val dataset = session.read.format("libsvm").load(datasetPath)

    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(dataset)

    // Train a DecisionTree model.
    val dt = new DecisionTreeRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    // Chain indexers and tree in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, dt))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(dataset)

    model.write.overwrite().save(savePath)
    Map.empty
  }

  def serve(modelPath: String, features: List[Map[String, Any]]): Map[String, Any] = {
    import LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(LocalDataColumn("features", features.map(constructVector)))

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("prediction").toMapList)
  }
}
