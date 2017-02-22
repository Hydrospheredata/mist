import io.hydrosphere.mist.lib._

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.linalg.Vectors


object MinMaxScaler extends MLMistJob with SQLSupport {
  def train(): Map[String, Any] = {

    val dataFrame = session.createDataFrame(Seq(
      (0, Vectors.dense(1.0, 0.1, -1.0)),
      (1, Vectors.dense(2.0, 1.1, 1.0)),
      (2, Vectors.dense(3.0, 10.1, 3.0))
    )).toDF("id", "features")

    val scaler = new MinMaxScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(dataFrame)

    model.write.overwrite().save("models/minmaxscaler")
    Map.empty[String, Any]
  }

  def serve(features: List[Double]): Map[String, Any] = {
    import io.hydrosphere.mist.ml.transformers.LocalTransformers._

    val features = List(
      Array(2.0, 0.0, 3.0),
      Array(4.0, 0.0, 0.0)
    )

    val pipeline = PipelineLoader.load("models/minmaxscaler")
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("scaledFeatures").toMapList)
  }
}
