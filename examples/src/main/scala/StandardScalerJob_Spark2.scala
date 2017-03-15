import io.hydrosphere.mist.lib._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vectors


object StandardScalerJob extends MLMistJob with SQLSupport {
  def train(savePath: String): Map[String, Any] = {
    val data = Array(
      Vectors.dense(0.0, 10.3, 1.0, 4.0, 5.0),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    )
    val df = session.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    val pipeline = new Pipeline().setStages(Array(scaler))

    val model = pipeline.fit(df)

    model.write.overwrite().save(savePath)
    Map.empty[String, Any]
  }

  def serve(modelPath: String, features: List[Array[Double]]): Map[String, Any] = {
    import io.hydrosphere.mist.lib.spark2.ml.transformers.LocalTransformers._

    val pipeline = PipelineLoader.load(modelPath)
    val data = LocalData(
      LocalDataColumn("features", features)
    )

    val result: LocalData = pipeline.transform(data)
    Map("result" -> result.select("features", "scaledFeatures").toMapList)
  }
}
