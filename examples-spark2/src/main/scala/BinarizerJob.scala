import io.hydrosphere.mist.lib.spark2._
import io.hydrosphere.mist.lib.spark2.ml.LocalTransformers
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.Binarizer


object BinarizerJob extends MLMistJob with SQLSupport {

  def train(savePath: String): Map[String, Any] = {
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = session.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(5.0)

    val pipeline = new Pipeline().setStages(Array(binarizer))

    val model = pipeline.fit(dataFrame)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Double]): Map[String, Any] = {
    import LocalTransformers._
    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("feature", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("feature", "binarized_feature").toMapList)
  }
}
