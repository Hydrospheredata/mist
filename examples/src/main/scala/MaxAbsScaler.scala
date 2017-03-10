import io.hydrosphere.mist.lib._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MaxAbsScaler
import org.apache.spark.ml.linalg.Vectors


object MaxAbsScaler extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    assert(savePath == "src/test/resources/models/maxabsscaler")

    val dataFrame = session.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -8.0)),
      (1, Vectors.dense(2.0, 1.0, -4.0)),
      (2, Vectors.dense(4.0, 10.0, 8.0))
    )).toDF("id", "features")

    val scaler = new MaxAbsScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(dataFrame)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Array[Double]]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("scaledFeatures").toMapList)
  }
}
