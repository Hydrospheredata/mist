import io.hydrosphere.mist.lib._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.GBTRegressor

object GBTRegressionJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    val data = session.read.format("libsvm").load("examples/resources/sample_libsvm_data.txt")

    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)

    val pipeline = new Pipeline()
      .setStages(Array(gbt))

    val model = pipeline.fit(data)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Any]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.LocalPipelineModel._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("prediction").toMapList)
  }
}
