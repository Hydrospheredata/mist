import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Binarizer


object Binarizer extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    assert(savePath == "src/test/resources/models/binarizer")
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = session.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(5.0)

    val pipeline = new Pipeline().setStages(Array(binarizer))

    val model = pipeline.fit(dataFrame)

    //model.write.overwrite().save("src/test/resources/models/binarizer")
    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Double]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("feature", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("feature", "binarized_feature").toMapList)
  }
}
